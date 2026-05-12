jq '.' examples/entrypoints/node_get/request.json \
| curl -sS -X POST 'https://graphregistry.graphcert.cede-apps.ch/api/nodes/get' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
