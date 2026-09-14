# REST API

GraphRegistry exposes a FastAPI REST API under the `/api` prefix. Interactive documentation is available at `/docs` (Swagger UI) and `/redoc` (ReDoc) when the API is running.

## Base URL

```
http://127.0.0.1:9999/api
```

The environment is selected once per API process via the `GRAPHREGISTRY_API_ENV` environment variable and defaults to `xaas_coresrv`.

## System endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check. |
| POST | `/api` | Status message with current environment. |

## Node endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/nodes/list` | List node keys for a type. |
| POST | `/api/nodes/exists` | Check if one node exists. |
| POST | `/api/nodes/exists_many` | Check if many nodes exist. |
| POST | `/api/nodes/get` | Get one node. |
| POST | `/api/nodes/get_many` | Get many nodes. |
| POST | `/api/nodes/save` | Save one node. |
| POST | `/api/nodes/save_many` | Save many nodes. |
| POST | `/api/nodes/delete` | Delete one node. |
| POST | `/api/nodes/delete_many` | Delete many nodes. |

## Edge endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/edges/list` | List edge keys for a type pair. |
| POST | `/api/edges/exists` | Check if one edge exists. |
| POST | `/api/edges/exists_many` | Check if many edges exist. |
| POST | `/api/edges/get` | Get one edge. |
| POST | `/api/edges/get_many` | Get many edges. |
| POST | `/api/edges/save` | Save one edge. |
| POST | `/api/edges/save_many` | Save many edges. |
| POST | `/api/edges/delete` | Delete one edge. |
| POST | `/api/edges/delete_many` | Delete many edges. |

## Example: save a node

```bash
curl -X POST http://127.0.0.1:9999/api/nodes/save \
  -H "Content-Type: application/json" \
  -d '{
    "node": {
      "type": "Course",
      "id": "graph-101",
      "title": "Knowledge Graphs",
      "description": "An introductory course."
    }
  }'
```

## Example: save an edge

```bash
curl -X POST http://127.0.0.1:9999/api/edges/save \
  -H "Content-Type: application/json" \
  -d '{
    "edge": {
      "from_type": "Person",
      "from_id": "alice",
      "to_type": "Course",
      "to_id": "graph-101",
      "context": "teacher"
    }
  }'
```

## Allowed types

The API rejects node/edge types that are not listed in `config/application/config_api.json` with HTTP 400.

## Error responses

The API returns structured JSON errors:

```json
{
  "detail": "Node type 'UnknownType' is not an allowed type."
}
```

See the interactive Swagger UI for full schemas and examples.
