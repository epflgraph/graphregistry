jq '.' examples/entrypoints/node_list/request.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/nodes/list' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
