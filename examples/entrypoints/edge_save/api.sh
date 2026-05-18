jq '.' examples/entrypoints/edge_save/request.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/edges/save' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
