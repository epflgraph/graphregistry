jq '.' examples/entrypoints/node_save_many/request.json \
| curl -sS -X POST 'https://graphregistry.graphcert.cede-apps.ch/api/nodes/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
