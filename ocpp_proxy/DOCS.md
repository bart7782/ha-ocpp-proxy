# OCPP Proxy

A passive OCPP 1.6J proxy between your charger and its backend. Every message is
forwarded unchanged; Home Assistant gets `sensor.autel_*` and `binary_sensor.autel_verbonden`.

## Options

- `upstream`: the backend without a path, e.g. `ws://ocpp.e-flux.nl` (Vattenfall).
- `tags`: list of `unknown_tag=accepted_tag`. A scan of `unknown_tag` is replaced by
  `accepted_tag` in Authorize/StartTransaction/StopTransaction towards the backend.
  Leave empty for a pure passthrough.
- `log`: message log in `/data/ocpp-proxy.jsonl` (contains idTags, max 2 x 5 MB).
- `ha_url`, `ha_token`: only fill these in if Supervisor access does not work
  (use a long-lived access token).

## Setup

1. Start the add-on and check the log for `proxy listening on port 9000`.
2. Point the charger at `ws://<HA-ip>:9000/<same path as before>/` (mind the trailing slash).
3. The log shows `charger connected` and the sensors appear.

If the proxy or HA is down, the charger cannot reach its backend. To bypass the
proxy, put the original server URL back in the charger app.
