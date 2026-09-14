# Quickstart

This guide walks through a minimal GraphRegistry workflow using the provided sample data. You do not need GraphAI or GraphOntology running for this test run because pre-calculated samples are included.

## 1. Prepare configuration

Copy the example configuration files and fill in the real connection details:

```bash
cp -r config.examples config
# Edit config/config_global.yaml, config/config_db.yaml, config/config_es.yaml, etc.
```

## 2. Test the CLI

```bash
graphregistry test
graphregistry -h
```

## 3. Validate configuration

```bash
graphregistry config validate
graphregistry config show
```

## 4. Import sample nodes and edges

Sample files are available under `scripts/init/sample_sets/`.

```bash
graphregistry data insert \
  --node_list=@scripts/init/sample_sets/epfl_graph_sample_set_NODEs.json \
  --actions=commit

graphregistry data insert \
  --edge_list=@scripts/init/sample_sets/epfl_graph_sample_set_EDGEs.json \
  --actions=commit
```

## 5. Check the API

Start the API and query the health endpoint:

```bash
curl -X GET http://127.0.0.1:9999/health 2>/dev/null | jq
```

You can also create nodes through the API:

```bash
curl -X POST http://127.0.0.1:9999/api/nodes/save \
  -H "Content-Type: application/json" \
  -d '{
    "node": {
      "type": "Course",
      "id": "my-course-101",
      "title": "Introduction to Graph Data",
      "description": "A sample course node."
    }
  }'
```

## 6. Run a full processing cycle

A typical cycle refreshes cache tables, propagates airflow flags, and rebuilds Elasticsearch indexes. The exact commands depend on your deployment; see the [CLI reference](../cli/index.md) for details.

```bash
graphregistry airflow reset --options typeflags airflow cache
graphregistry run --full
graphregistry index build
```

!!! tip
    Start with `actions=eval` instead of `actions=commit` to preview changes before writing them.
