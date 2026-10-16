# -*- coding: utf-8 -*-
"""Passive OCPP proxy between an EV charger and its backend; HA only observes.

    python ocpp_proxy.py --upstream wss://<backend-url> [--port 9000]
    python ocpp_proxy.py --test

The charger connects to this proxy (ws://<host>:9000/<charge-point-id>), the
proxy connects to the backend. Every message is passed through UNCHANGED, in
both directions: the original text frame is forwarded, nothing is filtered,
replaced or sent on its own. The proxy never answers a backend command itself.
A copy of every message goes to an observer that turns it into sensors
(sensor.autel_*, via the Home Assistant REST API). The observer catches all of
its own errors, so a bug in it cannot affect forwarding.

If the backend goes away, the proxy closes the connection to the charger so the
charger reconnects on its own. If the proxy goes away, the charger can no longer
reach the backend: run it under a supervisor that restarts it (systemd with
Restart=always, or as a Home Assistant add-on), and point the charger back at
the backend directly to bypass the proxy.

Single exception (only when OCPP_TAGS is set): a tag from that list is replaced,
in Authorize/StartTransaction/StopTransaction from the charger to the backend,
by a tag the backend knows. Everything else passes through byte-identical.

Only OCPP 1.6J is parsed; 2.0.1 is passed through but produces no sensors.
The message log (OCPP_LOGFILE, default ocpp-proxy.jsonl next to this file)
contains idTags.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pathlib
import sys
import time

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

SUBPROTOCOLS = ['ocpp1.6', 'ocpp2.0.1']
LOGFILE = pathlib.Path(os.environ.get('OCPP_LOGFILE') or
                       pathlib.Path(__file__).resolve().parent / 'ocpp-proxy.jsonl')
LOG_MAX_BYTES = 5_000_000
log = logging.getLogger('ocpp-proxy')


TAG_ACTIONS = ('Authorize', 'StartTransaction', 'StopTransaction')


def parse_tags(text: str) -> dict[str, str]:
    """'unknown=accepted,unknown2=accepted' -> {unknown: accepted}; tags compare case-insensitively."""
    out = {}
    for part in (text or '').split(','):
        if '=' in part:
            unknown, accepted = (x.strip() for x in part.split('=', 1))
            if unknown and accepted:
                out[unknown.lower()] = accepted
    return out


def replace_tag(text: str, tags: dict[str, str]) -> tuple[str, str | None]:
    """Returns (text for the backend, replaced tag or None). Without a match the text is unchanged."""
    if not tags:
        return text, None
    try:
        b = json.loads(text)
        if (isinstance(b, list) and len(b) == 4 and b[0] == 2 and b[2] in TAG_ACTIONS
                and isinstance(b[3], dict) and str(b[3].get('idTag', '')).lower() in tags):
            original = b[3]['idTag']
            b[3]['idTag'] = tags[original.lower()]
            return json.dumps(b, separators=(',', ':')), original
    except (ValueError, TypeError):
        pass
    return text, None


class Observer:
    """Reads along and publishes the result to HA. Never affects forwarding."""

    # The sensor keys below form the HA entity IDs (sensor.autel_<key>). They are
    # kept as they were for compatibility with existing dashboards and automations.

    # measurand -> (sensor key, unit, device_class, conversion factor)
    MEASURANDS = {
        'Power.Active.Import': ('vermogen', 'W', 'power', 1),
        'Current.Import': ('stroom', 'A', 'current', 1),
        'Voltage': ('spanning', 'V', 'voltage', 1),
        'Energy.Active.Import.Register': ('meterstand', 'kWh', 'energy', 0.001),
        'SoC': ('accu', '%', 'battery', 1),
    }

    FRIENDLY_NAMES = {
        'status': 'Autel status',
        'fout': 'Autel error',
        'model': 'Autel model',
        'firmware': 'Autel firmware',
        'vermogen': 'Autel power',
        'stroom': 'Autel current',
        'spanning': 'Autel voltage',
        'meterstand': 'Autel meter reading',
        'sessie_energie': 'Autel session energy',
        'accu': 'Autel battery',
    }

    def __init__(self, to_ha=True):
        self.to_ha = to_ha
        self.pending: dict[str, str] = {}
        self.values: dict[str, tuple] = {}
        self.last_post: dict[str, tuple] = {}
        self.session_start_wh: float | None = None
        self.connected = False
        # Until a charger has connected to this process, the connected state is
        # not ours to publish: another proxy instance may own the charger.
        self.ever_connected = False

    def message(self, direction: str, text: str):
        try:
            self._process(direction, text)
        except Exception:
            log.exception('observer failed (ignored)')

    def _process(self, direction, text):
        try:
            b = json.loads(text)
        except ValueError:
            return
        if not isinstance(b, list) or len(b) < 3:
            return
        if b[0] == 2:
            self.pending[b[1]] = b[2]
            self._on_request(direction, b[2], b[3] if len(b) > 3 else {})
        elif b[0] == 3:
            self.pending.pop(b[1], None)

    def _on_request(self, direction, action, data):
        if direction != 'charger':
            return
        if action == 'StatusNotification':
            self._set('status', data.get('status'), None, None)
            self._set('fout', data.get('errorCode'), None, None)
        elif action == 'MeterValues':
            self._meter_values(data)
        elif action == 'StartTransaction':
            self.session_start_wh = data.get('meterStart')
        elif action == 'StopTransaction':
            self._set('vermogen', 0, 'W', 'power')
            self._set('stroom', 0, 'A', 'current')
            self.session_start_wh = None
            self._set('sessie_energie', 0, 'kWh', 'energy')
        elif action == 'BootNotification':
            self._set('model', data.get('chargePointModel'), None, None)
            self._set('firmware', data.get('firmwareVersion'), None, None)

    def _meter_values(self, data):
        for mv in data.get('meterValue', []):
            for s in mv.get('sampledValue', []):
                name = s.get('measurand', 'Energy.Active.Import.Register')
                if name not in self.MEASURANDS or s.get('phase'):
                    continue
                sensor, unit, device_class, factor = self.MEASURANDS[name]
                try:
                    v = float(s['value'])
                except (KeyError, ValueError):
                    continue
                if s.get('unit', '').startswith('k') and name != 'Energy.Active.Import.Register':
                    v *= 1000
                if name == 'Energy.Active.Import.Register':
                    if s.get('unit') == 'kWh':
                        v *= 1000
                    if self.session_start_wh is not None:
                        self._set('sessie_energie', round((v - self.session_start_wh) / 1000, 3),
                                  'kWh', 'energy')
                self._set(sensor, round(v * factor, 3), unit, device_class)

    def _set(self, sensor, value, unit, device_class):
        if value is None:
            return
        self.values[sensor] = (value, unit, device_class)

    def connection(self, up: bool):
        self.connected = up
        if up:
            self.ever_connected = True

    def pending_posts(self) -> list[tuple[str, object, dict]]:
        """The states that changed since the last call, as (entity_id, state, attributes)."""
        posts = []
        state = ('on' if self.connected else 'off',)
        if self.ever_connected and self.last_post.get('_connected') != state:
            self.last_post['_connected'] = state
            posts.append(('binary_sensor.autel_verbonden', state[0],
                          {'friendly_name': 'Autel connected to proxy',
                           'device_class': 'connectivity'}))
        for name, (v, unit, device_class) in list(self.values.items()):
            if self.last_post.get(name) == (v,):
                continue
            self.last_post[name] = (v,)
            attrs = {'friendly_name': self.FRIENDLY_NAMES.get(name, f'Autel {name}')}
            if unit:
                attrs['unit_of_measurement'] = unit
                attrs['state_class'] = ('total_increasing' if name == 'meterstand'
                                        else 'measurement')
            if device_class:
                attrs['device_class'] = device_class
            posts.append((f'sensor.autel_{name}', v, attrs))
        return posts

    async def publish(self, stop: asyncio.Event):
        """Every 5 s, send the changed values (and the connection state) to HA."""
        if not self.to_ha:
            return
        import requests
        server, token = os.environ.get('OCPP_HA_URL'), os.environ.get('OCPP_HA_TOKEN')
        if not (server and token):
            log.warning('OCPP_HA_URL / OCPP_HA_TOKEN not set; not publishing to Home Assistant')
            return
        server = server.rstrip('/')
        headers = {'Authorization': f'Bearer {token}'}
        loop = asyncio.get_running_loop()

        def post(entity_id, state, attrs):
            try:
                requests.post(f'{server}/api/states/{entity_id}', headers=headers, timeout=10,
                              json={'state': state, 'attributes': attrs})
            except requests.RequestException as e:
                log.warning('Home Assistant unreachable? %s', e)

        while not stop.is_set():
            for p in self.pending_posts():
                await loop.run_in_executor(None, post, *p)
            try:
                await asyncio.wait_for(stop.wait(), 5)
            except asyncio.TimeoutError:
                pass


def write_log(direction, text):
    try:
        LOGFILE.parent.mkdir(exist_ok=True)
        if LOGFILE.exists() and LOGFILE.stat().st_size > LOG_MAX_BYTES:
            LOGFILE.replace(LOGFILE.with_suffix('.jsonl.1'))
        with LOGFILE.open('a', encoding='utf-8') as f:
            f.write(json.dumps({'t': time.strftime('%Y-%m-%dT%H:%M:%S'), 'from': direction,
                                'message': text}) + '\n')
    except OSError:
        pass


def make_handler(upstream: str, observer: Observer, logging_on: bool, tags: dict | None = None):
    async def handler(charger):
        path = charger.request.path
        target = upstream.rstrip('/') + path
        headers = {}
        if 'Authorization' in charger.request.headers:
            headers['Authorization'] = charger.request.headers['Authorization']
        sub = charger.subprotocol
        log.info('charger connected: %s -> %s', path, upstream)
        try:
            async with connect(target, subprotocols=[sub] if sub else None,
                               additional_headers=headers, ping_interval=30,
                               max_size=None) as up:
                observer.connection(True)

                async def pump(source, destination, direction):
                    async for text in source:
                        out = text
                        if direction == 'charger' and isinstance(text, str):
                            out, original = replace_tag(text, tags)
                            if original:
                                log.info('tag replaced for the backend: %s... -> %s...',
                                         original[:4], tags[original.lower()][:4])
                        await destination.send(out)
                        if isinstance(text, str):
                            if logging_on:
                                write_log(direction, text)
                            observer.message(direction, text)

                tasks = [asyncio.ensure_future(pump(charger, up, 'charger')),
                         asyncio.ensure_future(pump(up, charger, 'backend'))]
                done, rest = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in rest:
                    t.cancel()
                for t in done:
                    t.exception()
        except (OSError, websockets.exceptions.WebSocketException) as e:
            log.warning('connection lost or backend unreachable: %s', e)
            await charger.close(1013, 'upstream unreachable')
        finally:
            observer.connection(False)
            log.info('charger disconnected')
    return handler


async def run(upstream, port, to_ha, logging_on, stop: asyncio.Event | None = None,
              observer: Observer | None = None, bound: asyncio.Future | None = None,
              tags: dict | None = None):
    stop = stop or asyncio.Event()
    o = observer or Observer(to_ha)
    async with serve(make_handler(upstream, o, logging_on, tags), '0.0.0.0', port,
                     subprotocols=SUBPROTOCOLS, ping_interval=30, max_size=None) as srv:
        if bound:
            bound.set_result(srv.sockets[0].getsockname()[1])
        log.info('proxy listening on port %s, upstream %s%s', port, upstream,
                 f', {len(tags)} tag(s) will be replaced' if tags else '')
        pub = asyncio.ensure_future(o.publish(stop))
        await stop.wait()
        pub.cancel()


async def selftest():
    """Fake CSMS + fake charger: identical forwarding, nothing added, observer reads along."""
    received_csms, received_charger = [], []

    async def csms(ws):
        received_csms.append(('path', ws.request.path, ws.request.headers.get('Authorization'),
                              ws.subprotocol))
        async for m in ws:
            received_csms.append(m)
            b = json.loads(m)
            await ws.send(json.dumps([3, b[1], {'status': 'Accepted'}]))
            if b[2] == 'StartTransaction':
                await ws.send('[2,"v1","SetChargingProfile",{"connectorId":1}]')

    async with serve(csms, '127.0.0.1', 0, subprotocols=['ocpp1.6']) as fake:
        up = f'ws://127.0.0.1:{fake.sockets[0].getsockname()[1]}/ocpp'
        stop = asyncio.Event()
        bound = asyncio.get_running_loop().create_future()
        o = Observer(to_ha=False)
        proxy = asyncio.ensure_future(run(up, 0, False, False, stop, o, bound))
        port = await bound
        messages = [
            '[2,"1","BootNotification",{"chargePointModel":"Maxi","firmwareVersion":"1.2"}]',
            '[2,"2","StatusNotification",{"connectorId":1,"status":"Charging","errorCode":"NoError"}]',
            '[2,"3","StartTransaction",{"connectorId":1,"idTag":"X","meterStart":1000,"timestamp":"t"}]',
            '[2,"4","MeterValues",{"connectorId":1,"meterValue":[{"timestamp":"t","sampledValue":'
            '[{"value":"3450","measurand":"Power.Active.Import","unit":"W"},'
            '{"value":"1750","measurand":"Energy.Active.Import.Register","unit":"Wh"},'
            '{"value":"15.0","measurand":"Current.Import","unit":"A"}]}]}]',
            '[2,"5","Heartbeat",{}]',
        ]
        async with connect(f'ws://127.0.0.1:{port}/CHARGER-1', subprotocols=['ocpp1.6'],
                           additional_headers={'Authorization': 'Basic abc'}) as charger:
            for m in messages:
                await charger.send(m)
                reply = await charger.recv()
                received_charger.append(reply)
                if '"StartTransaction"' in m:
                    received_charger.append(await charger.recv())
        await asyncio.sleep(0.2)
        stop.set()
        await proxy

    assert received_csms[0] == ('path', '/ocpp/CHARGER-1', 'Basic abc', 'ocpp1.6'), received_csms[0]
    assert received_csms[1:] == messages, 'messages were changed or are missing'
    assert '[2,"v1","SetChargingProfile",{"connectorId":1}]' in received_charger, \
        'backend command did not reach the charger unchanged'
    assert received_charger.count('[2,"v1","SetChargingProfile",{"connectorId":1}]') == 1
    assert o.values['status'][0] == 'Charging'
    assert o.values['vermogen'][0] == 3450
    assert o.values['sessie_energie'][0] == 0.75
    assert o.values['stroom'][0] == 15.0
    assert o.values['model'][0] == 'Maxi'
    fresh = Observer(to_ha=False)
    assert not any(e == 'binary_sensor.autel_verbonden' for e, _, _ in fresh.pending_posts()), \
        'connected state published before any charger connected'
    fresh.connection(True)
    assert [s for e, s, _ in fresh.pending_posts() if e == 'binary_sensor.autel_verbonden'] == ['on']
    fresh.connection(False)
    assert [s for e, s, _ in fresh.pending_posts() if e == 'binary_sensor.autel_verbonden'] == ['off']
    assert replace_tag(messages[2], {})[0] == messages[2]
    t, original = replace_tag('[2,"9","Authorize",{"idTag":"AbC"}]', {'abc': 'REAL'})
    assert original == 'AbC' and json.loads(t) == [2, '9', 'Authorize', {'idTag': 'REAL'}], t
    t, original = replace_tag('[2,"9","StartTransaction",{"connectorId":1,"idTag":"abc","meterStart":1}]', {'abc': 'REAL'})
    assert json.loads(t)[3] == {'connectorId': 1, 'idTag': 'REAL', 'meterStart': 1}
    for untouched in ('[2,"9","Authorize",{"idTag":"OTHER"}]', '[2,"9","Heartbeat",{}]',
                      '[3,"9",{"idTagInfo":{"status":"Accepted"}}]', 'not json'):
        assert replace_tag(untouched, {'abc': 'REAL'}) == (untouched, None)
    print('selftest ok: 5 messages forwarded byte-identical, backend command reached the '
          'charger unchanged and exactly once, observer reads along')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--upstream', help='ws(s):// address of the backend, without the charge point ID')
    ap.add_argument('--port', type=int, default=9000, help='port the charger connects to (default 9000)')
    ap.add_argument('--no-ha', action='store_true', help='do not send anything to Home Assistant')
    ap.add_argument('--no-log', action='store_true', help='do not write the message log')
    ap.add_argument('--test', action='store_true', help='run the offline selftest and exit')
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if a.test:
        asyncio.run(selftest())
        return
    if not a.upstream:
        sys.exit('--upstream is missing (the ws(s):// address of the backend, without the ID)')
    asyncio.run(run(a.upstream, a.port, not a.no_ha, not a.no_log,
                    tags=parse_tags(os.environ.get('OCPP_TAGS', ''))))


if __name__ == '__main__':
    main()
