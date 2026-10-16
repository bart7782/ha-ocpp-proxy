# ha-ocpp-proxy

Home Assistant add-on: an OCPP 1.6J proxy between an EV charger (tested with an Autel
MaxiCharger AC) and its real backend (tested with Vattenfall/e-flux). Everything passes
through unchanged; HA gets sensors (status, power, current, voltage, meter reading, session
energy). Optionally a known tag can be substituted for another (see `ocpp_proxy/DOCS.md`).

## Install

Settings -> Add-ons -> Add-on store -> menu (three dots) -> Repositories ->
`https://github.com/bart7782/ha-ocpp-proxy`. (HACS cannot install add-ons.)
