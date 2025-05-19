Here you can find example queries to run once the registry API is running:

```bash
curl -X POST http://0.0.0.0:8000/registry/insert -H "Content-Type: application/json" -d @data/courses_example.json | jq .

curl -X POST http://0.0.0.0:8000/registry/insert -H "Content-Type: application/json" -d @data/accreditations_example.json | jq .
```