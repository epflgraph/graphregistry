# Configuration

GraphRegistry reads all configuration from a `config/` directory at the repository root. Example templates are provided in `config.examples/`.

## Environment configuration

### Global registry configuration

`config/config_global.yaml` controls execution mode, schema names, and service limits.

```yaml
api:
  title: "GraphRegistry API"
  summary: "HTTP API for Graph data management."

cli:
  limit_per_type_max: 10000

database:
  sql_paths:
    formulas: "/path/to/database/formulas/"
    exports:  "/path/to/data/exports/"
    patches:  "/path/to/data/patches/"

  schema_names:
    ontology: graph_ontology
    registry: graph_registry
    lectures: graph_lectures
    airflow: graph_airflow
    traversals: graph_traversals
    elasticsearch_cache: elasticsearch_cache
    graph_cache_test: graph_cache
    graph_cache_prod: graph_cache_prod
    graphsearch_test: graphsearch_test
    graphsearch_prod: graphsearch_prod
    graphsearch_prod_mirror: graphsearch_prod_mirror
    graphai_cache_api: graphai_cache_api

  mode: dev
```

!!! note "Dev mode"
    In `dev` mode, schema names are prefixed with `_1_DEV_` to avoid touching production tables.

### MySQL/MariaDB

`config/config_db.yaml` defines database environments used by the `graphdb` client.

```yaml
client_bin: /usr/bin/docker run --rm -i -e=MYSQL_PWD -v /path/to/data:/path/to/data mariadb:VERSION mariadb
dump_bin:   /usr/bin/docker run --rm -i -e=MYSQL_PWD -v /path/to/data:/path/to/data mariadb:VERSION mariadb-dump

export_path: /path/to/data/mysql_exports

environments:
  coresrv_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD
    ssl:
      ca: /path/to/ssl/ca-certificates.crt
      verify_server_cert: true

default_env: coresrv_env
```

### Elasticsearch

`config/config_es.yaml` defines Elasticsearch environments used by the `graphes` client.

```yaml
export_path: /path/to/data/elasticsearch_exports

environments:
  coresrv_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD

default_env: coresrv_env
```

### GraphAI

`config/config_graphai.yaml` (or the JSON variant referenced by `config_global.yaml`) stores GraphAI connection credentials.

```yaml
graphai:
  host_address: HOST_ADDRESS
  port: PORT
  username: USERNAME
  password: PASSWORD
```

### Generative AI (optional)

`config/config_genai.yaml` configures an optional LLM for semantic enrichment.

```yaml
genai:
  api_key: API_KEY
  inference_url: "https://api.example.com/v1"
  llm_model: "moonshotai/Kimi-K2.7-Code"
```

## Application configuration

### Allowed API types

`config/config_api.json` lists the node and edge types the REST API is allowed to accept.

```json
{
  "allowed-types": {
    "nodes": ["Course", "Lecture", "Person", "Publication", "Unit"],
    "edges": [
      ["Course", "Person", "teacher"],
      ["Lecture", "Course", "part of"],
      ["Person", "Unit", "accreditation"],
      ["Publication", "Person", "authorship"],
      ["Unit", "Unit", "affiliation"]
    ]
  }
}
```

### Airflow rules

`config/config_airflow.json` declares which node and edge types participate in each refresh cycle.

```json
{
  "nodes": [
    ["Category", true, true],
    ["Concept", true, true],
    ["Course", true, true]
  ],
  "edges": [
    ["Category", "Category", true],
    ["Course", "Lecture", true]
  ]
}
```

Each node tuple is `[type, process_fields, process_scores]`. Each edge tuple is `[from_type, to_type, process_fields]`.

### Semantic scoring

`config/config_scores.json` defines which node-to-node tuples should receive semantic scores.

```json
{
  "scored-edge-tuples": {
    "education": [
      ["Course", "Course"],
      ["Course", "Lecture"]
    ],
    "research": [
      ["Person", "Person"],
      ["Person", "Publication"]
    ]
  }
}
```

### Indexing setup

`config/config_index.json` controls Elasticsearch index layout, field selection, and ranking. Start from the provided example and adapt it to your use case.

## Validating configuration

Use the CLI to validate and inspect configuration files:

```bash
graphregistry config validate
graphregistry config show
```
