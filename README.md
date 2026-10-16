# ha-ocpp-proxy

> **Note:** All code in this repository was written with AI assistance. I built this for my own use on my own installation, and I'm sharing it in case it's useful to others too. It was written from scratch (no code taken from other projects); it relies on the `websockets` and `requests` libraries.

A Home Assistant add-on that sits as a passive OCPP 1.6J WebSocket proxy between an EV
charger and its backend.

## What it does

Many chargers that are supplied by an energy company are locked to that company's OCPP
backend (CSMS): you cannot point them at Home Assistant without losing billing, RFID
cards and the app. This add-on sits in between. The charger connects to the proxy, the
proxy connects to the real backend, and every message is forwarded in both directions
exactly as it was sent. A read-only observer looks at a copy of the traffic and turns it
into Home Assistant sensors, so you get live data without the backend noticing anything.

```
                  OCPP 1.6J                        OCPP 1.6J
 +---------+   ws://<HA-ip>:9000/...   +-------+   ws(s)://backend/...   +---------+
 | Charger | <-----------------------> | Proxy | <---------------------> | Backend |
 +---------+                           +-------+                         +---------+
                                           |  copy of every message
                                           v
                                  +-----------------+
                                  |    Observer     |
                                  +-----------------+
                                           |  REST API
                                           v
                               Home Assistant sensors
                     sensor.autel_*  /  binary_sensor.autel_verbonden
```

## Features

- **Byte-identical passthrough.** The original text frame is forwarded; nothing is
  filtered, rewritten, added or answered by the proxy (the only exception is the optional
  tag replacement below). The charger's path, `Authorization` header and OCPP subprotocol
  are passed on to the backend.
- **The observer cannot affect forwarding.** It catches all of its own errors, and HA
  being slow or unreachable only delays the sensors, never the messages.
- **Sensors**, updated at most every 5 seconds and only when a value changes:

  | Entity | Friendly name | Unit |
  | --- | --- | --- |
  | `binary_sensor.autel_verbonden` | Autel connected to proxy | on/off (connectivity) |
  | `sensor.autel_status` | Autel status | text (`Available`, `Charging`, ...) |
  | `sensor.autel_fout` | Autel error | text (OCPP `errorCode`) |
  | `sensor.autel_model` | Autel model | text |
  | `sensor.autel_firmware` | Autel firmware | text |
  | `sensor.autel_vermogen` | Autel power | W |
  | `sensor.autel_stroom` | Autel current | A |
  | `sensor.autel_spanning` | Autel voltage | V |
  | `sensor.autel_meterstand` | Autel meter reading | kWh (`total_increasing`) |
  | `sensor.autel_sessie_energie` | Autel session energy | kWh |
  | `sensor.autel_accu` | Autel battery | % (only if the charger reports SoC) |

  The entity IDs are Dutch because that is how this started; they are kept as they are
  for compatibility with existing dashboards and automations. Only the friendly names
  are English.
- **Optional tag replacement** (see [Tag replacement](#tag-replacement)).
- **Rotating message log** in `/data/ocpp-proxy.jsonl` (one JSON object per message,
  rotated at 5 MB, so at most 2 x 5 MB on disk).
- **Watchdog.** The Supervisor checks port 9000 and restarts the add-on if it stops
  listening.

## Use cases

- See the live power, energy and state of a locked-down charger in Home Assistant and
  use the meter reading in the Energy dashboard.
- Keep the utility's backend working (billing, RFID cards, their app) while you measure.
- Build solar-surplus automations based on the sensors (for example, notify when the
  car is charging from the grid while you export solar power).
- Scan a spare or unknown RFID tag that the backend rejects, by substituting a tag it
  knows.
- Debug OCPP traffic: the jsonl log shows every message in both directions.

**Limits:**

- It is not a full CSMS. It cannot start, stop or throttle charging, and it never
  answers a backend command itself.
- Only OCPP 1.6J is parsed. OCPP 2.0.1 is passed through, but produces no sensors.
- The entity names assume one charger.

## Setup guide

1. **Add the repository.** In Home Assistant go to Settings -> Add-ons -> Add-on store ->
   menu (three dots, top right) -> Repositories, and add
   `https://github.com/bart7782/ha-ocpp-proxy`. (HACS cannot install add-ons.)
2. **Install** the "OCPP Proxy" add-on.
3. **Configure** it on the Configuration tab:

   | Option | Type | Default | Description |
   | --- | --- | --- | --- |
   | `upstream` | string | `ws://ocpp.e-flux.nl` | The backend's WebSocket address **without** the path or charge point ID. Use the server from your charger's current OCPP settings. |
   | `tags` | list | `[]` | Tag replacements as `unknown_tag=accepted_tag`. Leave empty for a pure passthrough. |
   | `log` | bool | `true` | Write the message log to `/data/ocpp-proxy.jsonl`. Contains idTags. |
   | `ha_url` | string | empty | Only if Supervisor access does not work: your HA URL, e.g. `http://192.168.1.10:8123`. |
   | `ha_token` | password | empty | Only together with `ha_url`: a long-lived access token. |

4. **Start** the add-on and check its log for `proxy listening on port 9000`.
5. **Point the charger at the proxy.** In the charger's OCPP settings, replace the
   server URL with `ws://<HA-ip>:9000/<same path as before>/`. If the original URL was
   `ws://ocpp.example.com/ocpp/`, set `upstream` to `ws://ocpp.example.com` and the
   charger to `ws://<HA-ip>:9000/ocpp/`. The trailing slash matters: many chargers
   append their charge point ID directly to the URL.
6. **Verify.** The add-on log shows `charger connected: /ocpp/<ID> -> ...`, and the
   sensors appear in Home Assistant (Developer tools -> States, search `autel`).
7. **Rollback:** put the original server URL back in the charger app. The proxy is then
   out of the loop.

## Charger notes

From real experience with an Autel MaxiCharger AC (EC22SC01):

- It may use its SIM (4G) for OCPP by default, in which case it never reaches your
  local network. Filling in dummy values in the APN fields in the Autel app makes it
  fall back to wifi. An empty APN means automatic SIM again.
- The Autel app may show a "configuration failed" error even though the charger does
  connect. This is cosmetic; check the add-on log instead.
- With OCPP enabled, local tag management in the app is unavailable.
- Give the charger and Home Assistant fixed IP addresses (DHCP reservations), otherwise
  the connection breaks when an address changes.

## Tag replacement

- Option format: `unknown_tag=accepted_tag`, one entry per list item. Tags are compared
  case-insensitively.
- Only `Authorize`, `StartTransaction` and `StopTransaction` sent from the charger to the
  backend are changed. Messages from the backend, and all other messages, pass through
  untouched. The log keeps the original tag as the charger sent it.
- **Security:** the log contains idTags, so treat it as sensitive. Tag replacement works
  around the backend's access control, so only use tags you are entitled to use.

## Troubleshooting

- **No `charger connected` in the log.** The charger is not reaching the proxy. Check the
  URL (`ws://`, not `wss://`, port 9000, trailing slash), the IP address of HA, and
  whether the charger is using 4G instead of wifi (see Charger notes).
- **`connection lost or backend unreachable`.** The proxy could not reach `upstream`.
  Check the address and that it has no path; the path comes from the charger.
- **The backend rejects the charger.** The path is probably wrong: the proxy appends the
  charger's path to `upstream`, so the result must equal the original URL plus the
  charge point ID.
- **Sensors don't appear.** Look for `Home Assistant unreachable?` in the log. If
  Supervisor access does not work, fill in `ha_url` and `ha_token`. The connected
  sensor only appears once a charger has connected since the add-on started.
- **Sensors are `unknown` after an HA restart.** States posted through the REST API are
  not restored by HA; they return with the next message from the charger.

## Development

Run the offline selftest (a fake backend and a fake charger, no network or HA needed):

```sh
python -m venv venv
venv/bin/pip install websockets requests
venv/bin/python ocpp_proxy/ocpp_proxy.py --test
```

Run it outside Home Assistant:

```sh
OCPP_HA_URL=http://<HA-ip>:8123 OCPP_HA_TOKEN=<token> \
  python ocpp_proxy/ocpp_proxy.py --upstream ws://ocpp.example.com [--port 9000] [--no-ha] [--no-log]
```

`OCPP_TAGS` (`unknown=accepted,...`) and `OCPP_LOGFILE` are read from the environment too.

Repository layout:

```
repository.yaml          add-on repository metadata
ocpp_proxy/
  config.yaml            add-on manifest (options, port, watchdog)
  Dockerfile             image: python + websockets + requests
  run.py                 reads /data/options.json and starts the proxy
  ocpp_proxy.py          the proxy, the observer and the selftest
  DOCS.md                documentation shown in the add-on UI
CHANGELOG.md
LICENSE
```

## Disclaimer

This project is not affiliated with Autel, Vattenfall or e-flux. Use it at your own
risk. While Home Assistant or the add-on is down, the charger cannot reach its backend,
which may mean RFID cards are not accepted and sessions are not billed until it is back
(or until you point the charger at the backend directly again).

## Credits and license

Written by bart7782, with AI assistance. Built on
[websockets](https://github.com/python-websockets/websockets) and
[requests](https://github.com/psf/requests). Released under the [MIT License](LICENSE).
