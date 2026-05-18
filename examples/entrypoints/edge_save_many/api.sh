jq '.' examples/entrypoints/edge_save_many/request.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/edges/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
