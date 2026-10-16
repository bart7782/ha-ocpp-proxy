# Changelog

## 1.1.0

- Remote start and stop: the proxy now exposes a local HTTP API on port 9001
  (`/start`, `/stop`, `/status`).
- `/start` sends `RemoteStartTransaction` to the charger with the configured
  backend tag, starting a standard session towards the backend.
- `/stop` sends `RemoteStopTransaction` with the active transaction ID.
- Replies to proxy-originated commands are handled internally and not forwarded
  to the backend.

## 1.0.1

- Fix: `binary_sensor.autel_verbonden` is no longer set to `off` at startup. The
  connected state is only published once a charger has connected to this process,
  so a restarting (or second) proxy instance no longer overwrites an `on` set by
  the instance the charger is actually connected to. After the first connection,
  every change (on/off) is published as before.
- The add-on, its documentation and its log messages are now in English. CLI flags
  are `--port`, `--no-ha` and `--no-log`; the log file env var is `OCPP_LOGFILE`.
  Entity IDs and add-on options are unchanged; friendly names are now English.

## 1.0.0

- First release.
