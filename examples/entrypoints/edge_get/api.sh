jq '.' examples/entrypoints/edge_get/request.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/edges/get' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
