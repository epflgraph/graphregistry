jq '.' bug2.json \
| curl -sS -X POST 'https://graphregistry.graphcert.cede-apps.ch/api/nodes/save' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
