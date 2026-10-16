# OCPP Proxy

Zet je laadpaal naar `ws://<HA-ip>:9000/<zelfde pad als bij je backend>/` (let op de slash
aan het eind). De proxy geeft alles door naar `upstream` en maakt er `sensor.autel_*` en
`binary_sensor.autel_verbonden` van.

## Opties

- `upstream`: de backend zonder pad, bijv. `ws://ocpp.e-flux.nl` (Vattenfall).
- `tags`: lijst `extra=echt`. Een scan van `extra` wordt in Authorize/Start/StopTransaction
  naar de backend vervangen door `echt`. Leeg laten voor een zuivere doorgeefluik.
- `log`: berichtenlog in `/data/ocpp-proxy.jsonl` (bevat idTag's, max 2x 5 MB).
- `ha_url`, `ha_token`: alleen invullen als de Supervisor-toegang niet werkt (long-lived token).

Valt de proxy of HA weg, dan kan de laadpaal zijn backend niet bereiken; zet in de app
het profiel terug naar de directe verbinding om de proxy te omzeilen.
