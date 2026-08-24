jq '.' scripts/bug2b.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/nodes/save' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
