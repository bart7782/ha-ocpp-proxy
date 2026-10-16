import json
import os
import subprocess
import sys

opt = json.load(open('/data/options.json'))
env = dict(os.environ)
env['OCPP_TAGS'] = ','.join(opt.get('tags') or [])
env['OCPP_LOGFILE'] = '/data/ocpp-proxy.jsonl'
if opt.get('ha_url') and opt.get('ha_token'):
    env['OCPP_HA_URL'], env['OCPP_HA_TOKEN'] = opt['ha_url'], opt['ha_token']
else:
    env['OCPP_HA_URL'] = 'http://supervisor/core'
    env['OCPP_HA_TOKEN'] = os.environ.get('SUPERVISOR_TOKEN', '')
cmd = [sys.executable, '-u', '/app/ocpp_proxy.py', '--upstream', opt['upstream']]
if not opt.get('log', True):
    cmd.append('--no-log')
sys.exit(subprocess.call(cmd, env=env))
