# -*- coding: utf-8 -*-
"""Passieve OCPP-proxy tussen de Autel-lader en Vattenfall; HA kijkt alleen mee.

    python tools/ocpp_proxy.py --upstream wss://<vattenfall-url> [--poort 9000]
    python tools/ocpp_proxy.py --test

De lader verbindt met deze proxy (ws://<claude-dev>:9000/<laadpaal-id>), de proxy
met Vattenfall. Elk bericht gaat ONGEWIJZIGD door, in beide richtingen: het
originele tekstframe wordt doorgegeven, niets wordt gefilterd, vervangen of
zelf verstuurd. De proxy beantwoordt nooit zelf een commando van Vattenfall.
Een kopie van elk bericht gaat naar een waarnemer die er sensoren van maakt
(sensor.autel_*, via de REST-API van HA). Die waarnemer vangt al zijn fouten
op, dus een bug daarin kan het doorgeven niet raken.

Valt Vattenfall weg, dan sluit de proxy de verbinding met de lader, zodat die
zelf opnieuw verbindt. Valt de proxy weg, dan kan de lader niet meer met
Vattenfall praten: draai hem daarom onder systemd met Restart=always
(of als add-on) en zet in de lader-app het profiel Vattenfall_EV terug om de proxy te omzeilen.

Enige uitzondering (alleen als OCPP_TAGS is gezet, zie docs/ocpp-proxy.md): een tag
uit die lijst wordt in Authorize/StartTransaction/StopTransaction van de lader
naar Vattenfall vervangen door een tag die Vattenfall kent. Al het andere gaat
byte-identiek door.

Alleen OCPP 1.6J wordt gelezen; 2.0.1 gaat wel door maar zonder sensoren.
Logs staan in reports/ocpp-proxy.jsonl (gitignored; bevat idTag's).
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

sys.path.insert(0, str(pathlib.Path(__file__).parent))

SUBPROTOCOLLEN = ['ocpp1.6', 'ocpp2.0.1']
LOGBESTAND = pathlib.Path(os.environ.get('OCPP_LOGBESTAND') or
                          pathlib.Path(__file__).resolve().parent / 'ocpp-proxy.jsonl')
LOG_MAX_BYTES = 5_000_000
log = logging.getLogger('ocpp-proxy')


TAGACTIES = ('Authorize', 'StartTransaction', 'StopTransaction')


def lees_tags(tekst: str) -> dict[str, str]:
    """'extra=echt,extra2=echt' -> {extra: echt}; tags worden hoofdletterongevoelig vergeleken."""
    uit = {}
    for deel in (tekst or '').split(','):
        if '=' in deel:
            extra, echt = (x.strip() for x in deel.split('=', 1))
            if extra and echt:
                uit[extra.lower()] = echt
    return uit


def vervang_tag(tekst: str, tags: dict[str, str]) -> tuple[str, str | None]:
    """Geeft (tekst voor Vattenfall, vervangen tag of None). Zonder treffer blijft tekst gelijk."""
    if not tags:
        return tekst, None
    try:
        b = json.loads(tekst)
        if (isinstance(b, list) and len(b) == 4 and b[0] == 2 and b[2] in TAGACTIES
                and isinstance(b[3], dict) and str(b[3].get('idTag', '')).lower() in tags):
            oud = b[3]['idTag']
            b[3]['idTag'] = tags[oud.lower()]
            return json.dumps(b, separators=(',', ':')), oud
    except (ValueError, TypeError):
        pass
    return tekst, None


class Waarnemer:
    """Leest mee en zet de uitkomst in HA. Raakt het doorgeven nooit."""

    # meetwaarde -> (sensor, eenheid, device_class, omrekenfactor)
    METINGEN = {
        'Power.Active.Import': ('vermogen', 'W', 'power', 1),
        'Current.Import': ('stroom', 'A', 'current', 1),
        'Voltage': ('spanning', 'V', 'voltage', 1),
        'Energy.Active.Import.Register': ('meterstand', 'kWh', 'energy', 0.001),
        'SoC': ('accu', '%', 'battery', 1),
    }

    def __init__(self, naar_ha=True):
        self.naar_ha = naar_ha
        self.open: dict[str, str] = {}
        self.waarden: dict[str, tuple] = {}
        self.laatste_post: dict[str, tuple] = {}
        self.sessie_start_wh: float | None = None
        self.verbonden = False
        self._ha = None

    def bericht(self, richting: str, tekst: str):
        try:
            self._verwerk(richting, tekst)
        except Exception:
            log.exception('waarnemer faalde (negeerbaar)')

    def _verwerk(self, richting, tekst):
        try:
            b = json.loads(tekst)
        except ValueError:
            return
        if not isinstance(b, list) or len(b) < 3:
            return
        if b[0] == 2:
            self.open[b[1]] = b[2]
            self._bij_verzoek(richting, b[2], b[3] if len(b) > 3 else {})
        elif b[0] == 3:
            self.open.pop(b[1], None)

    def _bij_verzoek(self, richting, actie, data):
        if richting != 'lader':
            return
        if actie == 'StatusNotification':
            self._zet('status', data.get('status'), None, None)
            self._zet('fout', data.get('errorCode'), None, None)
        elif actie == 'MeterValues':
            self._meterwaarden(data)
        elif actie == 'StartTransaction':
            self.sessie_start_wh = data.get('meterStart')
        elif actie == 'StopTransaction':
            self._zet('vermogen', 0, 'W', 'power')
            self._zet('stroom', 0, 'A', 'current')
            self.sessie_start_wh = None
            self._zet('sessie_energie', 0, 'kWh', 'energy')
        elif actie == 'BootNotification':
            self._zet('model', data.get('chargePointModel'), None, None)
            self._zet('firmware', data.get('firmwareVersion'), None, None)

    def _meterwaarden(self, data):
        for mv in data.get('meterValue', []):
            for s in mv.get('sampledValue', []):
                naam = s.get('measurand', 'Energy.Active.Import.Register')
                if naam not in self.METINGEN or s.get('phase'):
                    continue
                sensor, eenheid, klasse, factor = self.METINGEN[naam]
                try:
                    w = float(s['value'])
                except (KeyError, ValueError):
                    continue
                if s.get('unit', '').startswith('k') and naam != 'Energy.Active.Import.Register':
                    w *= 1000
                if naam == 'Energy.Active.Import.Register':
                    if s.get('unit') == 'kWh':
                        w *= 1000
                    if self.sessie_start_wh is not None:
                        self._zet('sessie_energie', round((w - self.sessie_start_wh) / 1000, 3),
                                  'kWh', 'energy')
                self._zet(sensor, round(w * factor, 3), eenheid, klasse)

    def _zet(self, sensor, waarde, eenheid, klasse):
        if waarde is None:
            return
        self.waarden[sensor] = (waarde, eenheid, klasse)

    def verbinding(self, aan: bool):
        self.verbonden = aan

    async def publiceer(self, stop: asyncio.Event):
        """Elke 5 s de gewijzigde waarden (en de verbinding) naar HA."""
        if not self.naar_ha:
            return
        import requests
        server, token = os.environ.get('OCPP_HA_URL'), os.environ.get('OCPP_HA_TOKEN')
        if not (server and token):
            import ha_ws
            server, token = ha_ws.omgeving()
        server = server.rstrip('/')
        kop = {'Authorization': f'Bearer {token}'}
        lus = asyncio.get_running_loop()

        def post(entiteit, state, attrs):
            try:
                requests.post(f'{server}/api/states/{entiteit}', headers=kop, timeout=10,
                              json={'state': state, 'attributes': attrs})
            except requests.RequestException as e:
                log.warning('HA bereikbaar? %s', e)

        while not stop.is_set():
            taken = []
            toestand = ('on' if self.verbonden else 'off',)
            if self.laatste_post.get('_verbonden') != toestand:
                self.laatste_post['_verbonden'] = toestand
                taken.append(('binary_sensor.autel_verbonden', toestand[0],
                              {'friendly_name': 'Autel-lader verbonden met proxy',
                               'device_class': 'connectivity'}))
            for naam, (w, eenheid, klasse) in list(self.waarden.items()):
                if self.laatste_post.get(naam) == (w,):
                    continue
                self.laatste_post[naam] = (w,)
                attrs = {'friendly_name': f'Autel {naam.replace("_", " ")}'}
                if eenheid:
                    attrs['unit_of_measurement'] = eenheid
                    attrs['state_class'] = ('total_increasing' if naam == 'meterstand'
                                            else 'measurement')
                if klasse:
                    attrs['device_class'] = klasse
                taken.append((f'sensor.autel_{naam}', w, attrs))
            for t in taken:
                await lus.run_in_executor(None, post, *t)
            try:
                await asyncio.wait_for(stop.wait(), 5)
            except asyncio.TimeoutError:
                pass


def schrijf_log(richting, tekst):
    try:
        LOGBESTAND.parent.mkdir(exist_ok=True)
        if LOGBESTAND.exists() and LOGBESTAND.stat().st_size > LOG_MAX_BYTES:
            LOGBESTAND.replace(LOGBESTAND.with_suffix('.jsonl.1'))
        with LOGBESTAND.open('a', encoding='utf-8') as f:
            f.write(json.dumps({'t': time.strftime('%Y-%m-%dT%H:%M:%S'), 'van': richting,
                                'bericht': tekst}) + '\n')
    except OSError:
        pass


def maak_handler(upstream: str, waarnemer: Waarnemer, loggen: bool, tags: dict | None = None):
    async def handler(lader):
        pad = lader.request.path
        doel = upstream.rstrip('/') + pad
        koppen = {}
        if 'Authorization' in lader.request.headers:
            koppen['Authorization'] = lader.request.headers['Authorization']
        sub = lader.subprotocol
        log.info('lader verbonden: %s -> %s', pad, upstream)
        try:
            async with connect(doel, subprotocols=[sub] if sub else None,
                               additional_headers=koppen, ping_interval=30,
                               max_size=None) as up:
                waarnemer.verbinding(True)

                async def pomp(bron, bestemming, richting):
                    async for tekst in bron:
                        uit = tekst
                        if richting == 'lader' and isinstance(tekst, str):
                            uit, oud = vervang_tag(tekst, tags)
                            if oud:
                                log.info('tag vervangen voor Vattenfall: %s... -> %s...',
                                         oud[:4], tags[oud.lower()][:4])
                        await bestemming.send(uit)
                        if isinstance(tekst, str):
                            if loggen:
                                schrijf_log(richting, tekst)
                            waarnemer.bericht(richting, tekst)

                taken = [asyncio.ensure_future(pomp(lader, up, 'lader')),
                         asyncio.ensure_future(pomp(up, lader, 'vattenfall'))]
                klaar, rest = await asyncio.wait(taken, return_when=asyncio.FIRST_COMPLETED)
                for t in rest:
                    t.cancel()
                for t in klaar:
                    t.exception()
        except (OSError, websockets.exceptions.WebSocketException) as e:
            log.warning('verbinding verbroken of Vattenfall onbereikbaar: %s', e)
            await lader.close(1013, 'upstream onbereikbaar')
        finally:
            waarnemer.verbinding(False)
            log.info('lader losgekoppeld')
    return handler


async def draai(upstream, poort, naar_ha, loggen, stop: asyncio.Event | None = None,
                waarnemer: Waarnemer | None = None, gebonden: asyncio.Future | None = None,
                tags: dict | None = None):
    stop = stop or asyncio.Event()
    w = waarnemer or Waarnemer(naar_ha)
    async with serve(maak_handler(upstream, w, loggen, tags), '0.0.0.0', poort,
                     subprotocols=SUBPROTOCOLLEN, ping_interval=30, max_size=None) as srv:
        if gebonden:
            gebonden.set_result(srv.sockets[0].getsockname()[1])
        log.info('proxy luistert op poort %s, upstream %s%s', poort, upstream,
                 f', {len(tags)} tag(s) worden vervangen' if tags else '')
        pub = asyncio.ensure_future(w.publiceer(stop))
        await stop.wait()
        pub.cancel()


async def zelftest():
    """Nepcsms + nepdlader: identiek doorgeven, niets erbij, waarnemer leest mee."""
    ontvangen_csms, ontvangen_lader = [], []

    async def csms(ws):
        ontvangen_csms.append(('pad', ws.request.path, ws.request.headers.get('Authorization'),
                               ws.subprotocol))
        async for m in ws:
            ontvangen_csms.append(m)
            b = json.loads(m)
            await ws.send(json.dumps([3, b[1], {'status': 'Accepted'}]))
            if b[2] == 'StartTransaction':
                await ws.send('[2,"v1","SetChargingProfile",{"connectorId":1}]')

    async with serve(csms, '127.0.0.1', 0, subprotocols=['ocpp1.6']) as fake:
        up = f'ws://127.0.0.1:{fake.sockets[0].getsockname()[1]}/ocpp'
        stop = asyncio.Event()
        gebonden = asyncio.get_running_loop().create_future()
        w = Waarnemer(naar_ha=False)
        proxy = asyncio.ensure_future(draai(up, 0, False, False, stop, w, gebonden))
        poort = await gebonden
        berichten = [
            '[2,"1","BootNotification",{"chargePointModel":"Maxi","firmwareVersion":"1.2"}]',
            '[2,"2","StatusNotification",{"connectorId":1,"status":"Charging","errorCode":"NoError"}]',
            '[2,"3","StartTransaction",{"connectorId":1,"idTag":"X","meterStart":1000,"timestamp":"t"}]',
            '[2,"4","MeterValues",{"connectorId":1,"meterValue":[{"timestamp":"t","sampledValue":'
            '[{"value":"3450","measurand":"Power.Active.Import","unit":"W"},'
            '{"value":"1750","measurand":"Energy.Active.Import.Register","unit":"Wh"},'
            '{"value":"15.0","measurand":"Current.Import","unit":"A"}]}]}]',
            '[2,"5","Heartbeat",{}]',
        ]
        async with connect(f'ws://127.0.0.1:{poort}/LAADPAAL-1', subprotocols=['ocpp1.6'],
                           additional_headers={'Authorization': 'Basic abc'}) as lader:
            for m in berichten:
                await lader.send(m)
                antwoord = await lader.recv()
                ontvangen_lader.append(antwoord)
                if '"StartTransaction"' in m:
                    ontvangen_lader.append(await lader.recv())
        await asyncio.sleep(0.2)
        stop.set()
        await proxy

    assert ontvangen_csms[0] == ('pad', '/ocpp/LAADPAAL-1', 'Basic abc', 'ocpp1.6'), ontvangen_csms[0]
    assert ontvangen_csms[1:] == berichten, 'berichten zijn gewijzigd of ontbreken'
    assert '[2,"v1","SetChargingProfile",{"connectorId":1}]' in ontvangen_lader, \
        'commando van Vattenfall bereikte de lader niet ongewijzigd'
    assert ontvangen_lader.count('[2,"v1","SetChargingProfile",{"connectorId":1}]') == 1
    assert w.waarden['status'][0] == 'Charging'
    assert w.waarden['vermogen'][0] == 3450
    assert w.waarden['sessie_energie'][0] == 0.75
    assert w.waarden['stroom'][0] == 15.0
    assert w.waarden['model'][0] == 'Maxi'
    assert vervang_tag(berichten[2], {})[0] == berichten[2]
    t, oud = vervang_tag('[2,"9","Authorize",{"idTag":"AbC"}]', {'abc': 'ECHT'})
    assert oud == 'AbC' and json.loads(t) == [2, '9', 'Authorize', {'idTag': 'ECHT'}], t
    t, oud = vervang_tag('[2,"9","StartTransaction",{"connectorId":1,"idTag":"abc","meterStart":1}]', {'abc': 'ECHT'})
    assert json.loads(t)[3] == {'connectorId': 1, 'idTag': 'ECHT', 'meterStart': 1}
    for onaangeroerd in ('[2,"9","Authorize",{"idTag":"ANDER"}]', '[2,"9","Heartbeat",{}]',
                         '[3,"9",{"idTagInfo":{"status":"Accepted"}}]', 'geen json'):
        assert vervang_tag(onaangeroerd, {'abc': 'ECHT'}) == (onaangeroerd, None)
    print('zelftest ok: 5 berichten byte-identiek doorgegeven, commando van Vattenfall '
          'ongewijzigd en precies een keer bij de lader, waarnemer leest mee')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--upstream', help='ws(s)://-adres van Vattenfall, zonder laadpaal-ID')
    ap.add_argument('--poort', type=int, default=9000)
    ap.add_argument('--geen-ha', action='store_true', help='niets naar HA sturen')
    ap.add_argument('--geen-log', action='store_true', help='geen berichtenlog')
    ap.add_argument('--test', action='store_true')
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if a.test:
        asyncio.run(zelftest())
        return
    if not a.upstream:
        sys.exit('--upstream ontbreekt (het ws(s)://-adres van Vattenfall, zonder ID)')
    asyncio.run(draai(a.upstream, a.poort, not a.geen_ha, not a.geen_log,
                      tags=lees_tags(os.environ.get('OCPP_TAGS', ''))))


if __name__ == '__main__':
    main()
