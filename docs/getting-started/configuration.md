# Configuration

GraphRegistry reads all configuration from a `config/` directory at the repository root:

- `config/environment/` — connection settings for external services and core registry settings.
- `config/application/` — application behaviour rules.

Example templates are provided in `config.examples/`.

## Environment configuration

### Global registry configuration

`config/environment/config_registry.yml` controls execution mode, schema names, and service limits.

```yaml
api:
  title: "GraphRegistry API"
  summary: "HTTP API for Graph data management."

cli:
  limit_per_type_max: 10000

database:
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

`config/environment/config_graphdb.yml` defines database environments used by the `graphdb` client.

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

`config/environment/config_graphes.yml` defines Elasticsearch environments used by the `graphes` client.

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

`config/environment/config_graphai.yml` stores GraphAI connection credentials.

```yaml
graphai:
  host_address: "https://graphai.example.com"
  port: PORT
  username: USERNAME
  password: PASSWORD
```

### Generative AI (optional)

`config/environment/config_genai.yml` configures an optional LLM for semantic enrichment.

```yaml
genai:
  api_key: API_KEY
  inference_url: "https://api.example.com/v1"
  llm_model: "moonshotai/Kimi-K2.7-Code"
```

## Application configuration

### Allowed API types

`config/application/config_api.json` lists the node and edge types the REST API is allowed to accept.

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

`config/application/config_airflow.json` declares which node and edge types participate in each refresh cycle.

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

`config/application/config_scores.json` defines which node-to-node tuples should receive semantic scores.

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

`config/application/config_index.json` controls Elasticsearch index layout, field selection, and ranking. Start from the provided example and adapt it to your use case.

## Validating configuration

Use the CLI to validate and inspect configuration files:

```bash
# Validate all config files and their structure
graphregistry config validate

# Show a pretty-print summary of the registry configuration
graphregistry config show

# Show specific sections
graphregistry config show --index
graphregistry config show --scores
graphregistry config show --api

# Run a safe smoke test (connectivity checks)
graphregistry test
graphregistry test --skip-db --skip-es --skip-ai
```
