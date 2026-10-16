# ha-ocpp-proxy

Home Assistant add-on: een OCPP 1.6J-proxy tussen een laadpaal (getest met Autel MaxiCharger
AC) en de echte backend (getest met Vattenfall/e-flux). Alles gaat ongewijzigd door; HA krijgt
sensoren (status, vermogen, stroom, spanning, meterstand, sessie-energie). Optioneel kan een
bekende tag door een andere worden vervangen (zie `ocpp_proxy/DOCS.md`).

## Installeren

Instellingen -> Add-ons -> Add-on store -> menu (drie puntjes) -> Repositories ->
`https://github.com/bart7782/ha-ocpp-proxy`. (HACS kan geen add-ons installeren.)
