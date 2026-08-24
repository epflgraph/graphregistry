jq '.' scripts/bug2_edge_B.json \
| curl -sS -X POST 'https://graphregistry.graphcert.cede-apps.ch/api/edges/save' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
