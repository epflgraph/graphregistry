jq '.' examples/entrypoints/node_get_many/request.json \
| curl -sS -X POST 'https://graphregistry.graphcert.cede-apps.ch/api/nodes/get_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
