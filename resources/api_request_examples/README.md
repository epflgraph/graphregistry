Here you can find example queries to run from the graphregistry root directory once the registry API is running:

```bash
curl -X POST http://0.0.0.0:8000/registry/insert -H "Content-Type: application/json" -d @resources/api_request_examples/courses_example.json | jq .

curl -X POST http://0.0.0.0:8000/registry/insert -H "Content-Type: application/json" -d @resources/api_request_examples/accreditations_example.json | jq .

curl -X POST http://0.0.0.0:8000/registry/insert -H "Content-Type: application/json" -d @resources/api_request_examples/exercises_example.json | jq .
```