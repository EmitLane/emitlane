#!/usr/bin/env python3
"""Check dashboard wiring and emit recording rules for promtool syntax checks."""
import json
from pathlib import Path
import sys

dashboard = json.loads(Path(__file__).with_name('grafana-dashboard.json').read_text())
ids = set()
rules = []
for panel in dashboard['panels']:
    if panel['id'] in ids:
        raise ValueError('duplicate panel ID')
    ids.add(panel['id'])
    if panel['datasource'] != {'type': 'prometheus', 'uid': '${DS_PROMETHEUS}'}:
        raise ValueError('panel must use the selectable Prometheus datasource')
    for target in panel['targets']:
        expression = target['expr']
        for variable, value in {'$__rate_interval': '5m', '$__range': '1h', '$instance': '.*', '$job': 'emitlane'}.items():
            expression = expression.replace(variable, value)
        if '$' in expression:
            raise ValueError('unresolved dashboard variable')
        rules.append({'record': 'dashboard_panel_%s_%s' % (panel['id'], target['refId']), 'expr': expression})
if not rules:
    raise ValueError('dashboard has no queries')
json.dump({'groups': [{'name': 'dashboard-validation', 'rules': rules}]}, sys.stdout)
print()
