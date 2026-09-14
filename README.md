<img src="assets/icon-a0a7c0c9.png" alt="Project logo" height="64">

[![License](https://img.shields.io/github/license/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/blob/master/LICENSE)
[![Latest Release on Github](https://img.shields.io/github/v/release/epflgraph/graphregistry?sort=semver)](https://github.com/epflgraph/graphregistry/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/epflgraph/graphregistry?style=social)](https://github.com/epflgraph/graphregistry/stargazers)
[![Contributors](https://img.shields.io/github/contributors/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/commits/master)
[![Open Issues](https://img.shields.io/github/issues/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/issues)
[![Open PRs](https://img.shields.io/github/issues-pr/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/pulls)
=

🏠 [Project Home](https://github.com/epflgraph/graphproject) > Registry

**List of core services:**<br/>
Registry |
[AI](https://github.com/epflgraph/graphai/tree/rcp_deployment) |
[Ontology](https://github.com/epflgraph/graphontology) |
[Search](https://github.com/epflgraph/graphsearch_ui) |
[Chat](https://github.com/epflgraph/graphchatbot)

**List of utilities:**<br/>
[Dash](https://github.com/epflgraph/graphdashboard) |
[DB client](https://github.com/epflgraph/graphdb-client) |
[ES client](https://github.com/epflgraph/graphes-client) |
[SDK](https://github.com/epflgraph/graph-sdk) |
[Agents](https://github.com/epflgraph/graphagents)

<br />

Overview
========
**Graph Registry** is the first layer in the Graph Platform. It ingests data in JSON format through an ETL pipeline, and generates a knowledge graph that feeds the GraphSearch and GraphChat applications.

Data can be added to the registry through direct JSON file imports, or through a REST API. The actions steps in the knowledge graph construction are executed through a command line interface (CLI).

Installation
============

There are two methods of deploying the Graph Registry application: either with Docker or through a local Python installation. It is recommended you pull the repo first, and experiment with the multiple example scripts available.

In either case, you need to setup the configuration files before you can run the API or the CLI. This is explained in the next section.

## 🐳 Deploy with Docker

Graph Registry is available as a Docker image, which provides a convenient way to deploy the API and run the CLI without needing to set up a local Python environment. The image includes all necessary dependencies and can be easily updated by pulling the latest version from Docker Hub.

Steps to deploy with Docker:

1. Create a `docker-compose.yml` file with the following content:

    ```yaml
    services:
        graphregistry:
            image: epflgraph/graphregistry:latest
            container_name: graphregistry-app
            restart: unless-stopped

            ports:
                - "0493:0493"

            environment:
                GRAPHREGISTRY_ROLE: api
                API_HOST: 0.0.0.0
                API_PORT: 0493
                API_WORKERS: 1
                API_PROXY_HEADERS: 1
                API_FORWARDED_ALLOW_IPS: "*"

            volumes:
                - ./config:/app/config:ro
    ```

    This is the minimal configuration that will deploy and launch the application. A more complete example with HTTPS termination and log shipping is available in:<br />
    📂 [docker/deployment/docker-compose_https+monitor.yml](docker/deployment/docker-compose_https+monitor.yml)

2. Deploy the Registry app and other optional services:

    ```shell
    docker compose up -d
    ```

    If you have a running instance already, you can either execute a soft restart, or force a clean restart as follows:

    ```shell
    docker compose down graphregistry --remove-orphans
    docker rmi epflgraph/graphregistry:latest 2>/dev/null || true
    docker compose pull graphregistry
    docker compose up graphregistry -d --force-recreate
    ```

3. At this point, the API will already be running on the selected port. To run the CLI, you have to execute it inside the container:

    ```shell
    docker compose exec graphregistry graphregistry -h
    ```

To run commands as `graphregistry [cmd]`, add this to your `~/.zshrc` file:

```shell
graphregistry() {
  docker compose exec graphregistry graphregistry "$@"
}
```

Then reload your shell:

```shell
source ~/.zshrc
```

Test with:

```shell
graphregistry test
```

## 👨🏻‍💻 Local installation

For users who prefer to deploy the API and run the CLI directly on their local machine, follow these steps to set up a Python virtual environment and install the package:

1. Clone the repository:

    ```shell
    git clone https://github.com/epflgraph/graphregistry.git
    cd graphregistry
    ```

2. Create and activate a virtual environment:

    ```shell
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. Install the package:

    ```shell
    pip install .
    ```

4. Verify and test the CLI installation:

    ```shell
    graphregistry -h
    graphregistry test
    ```

5. Deploy the API:

    ```shell
    exec uvicorn graphregistry.entrypoints.api.main:create_app \
        --host 127.0.0.1 \
        --port 9999 \
        --workers 1 \
        --proxy-headers \
        --forwarded-allow-ips 127.0.0.1 \
        --factory
    ```

Once the API is running, the Swagger docs page becomes available here:<br />
🌍 [http://127.0.0.1:9999/docs](http://127.0.0.1:9999/docs)

You can also test it directly on your shell:

```shell
curl -X GET http://127.0.0.1:9999/health 2>/dev/null | jq
```

> [!IMPORTANT]
> Before any of these deployment methods works, you need to setup all required configuration files, as explained in the next section.

Configuration
=============
The Graph Registry application relies on seperate configuration files for each service it leverages or depends on. In addition, there is a number of configuration files governing different parts of the Registry workflow and the knowledge graph construction.

The configuration files should be stored in a folder named `config`. Example templates are provided in the folder: 📂 [config_examples](config_examples)

## Evironment setup

### Graph Registry

The global configuration for the Registry app is defined in: 📂 [config_registry.yml](config_examples/environment/config_registry.example.yml). The content resembles the following:

```yaml
# Title and summary description to be displayed on the API's Swagger page
api:
    title: "GraphRegistry API"
    summary: "HTTP API for Graph data management."

# CLI-related static variables
cli:
  limit_per_type_max: 10000

# Registry database-related configurations
database:

    # Path to folders containing SQL files to be exported, imported, and executed
    sql_paths:
        formulas: "/path/to/database/formulas/"
        exports:  "/path/to/data/exports/"
        patches:  "/path/to/data/patches/"

    # Custom DB schema names on MySQL/MariaDB (for the CREATE DATABASE <schema_name>; command)
    schema_names:
        ontology:                graph_ontology
        registry:                graph_registry
        lectures:                graph_lectures
        airflow:                 graph_airflow
        traversals:              graph_traversals
        elasticsearch_cache:     elasticsearch_cache
        graph_cache_test:        graph_cache
        graph_cache_prod:        graph_cache_prod
        graphsearch_test:        graphsearch_test
        graphsearch_prod:        graphsearch_prod
        graphsearch_prod_mirror: graphsearch_prod_mirror
        graphai_cache_api:       graphai_cache_api

    # Execution mode [dev, prod]
    # In 'dev' mode, data is written to separate DB schemas with the prefix "_1_DEV_", to avoid data being written to production tables.
    mode: dev
```

### MySQL/MariaDB

The configuration for the database connection and GraphDB CLI is defined in: 📂 [config_graphdb.yml](config_examples/environment/config_graphdb.example.yml). Your can define your multiple environments, which you then select with `graphdb --env <env_name>` in the CLI. The content resembles the following:

```yaml
# MySQL client and dump binaries
client_bin: /usr/bin/docker run --rm -i -e=MYSQL_PWD -v /path/to/data:/path/to/data mariadb:VERSION mariadb
dump_bin:   /usr/bin/docker run --rm -i -e=MYSQL_PWD -v /path/to/data:/path/to/data mariadb:VERSION mariadb-dump

# Default export path for database dumps
export_path: /path/to/data/mysql_exports

# Database environments (names must end in '_env')
environments:

  # Core services / test environment
  coresrv_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD
    # If SSL required
    ssl:
      ca: /path/to/ssl/ca-certificates.crt
      verify_server_cert: true

  # Prod environment
  prod_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD
    ssl:
      ca: /path/to/ssl/ca-certificates.crt
      verify_server_cert: true

  # Other environments
  something_else_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD

# Default environment to use when not specified in the GraphDB CLI.
default_env: coresrv_env
```

### ElasticSearch

The configuration for the ElasticSearch connection and GraphES CLI is defined in: 📂 [config_graphes.yml](config_examples/environment/config_graphes.example.yml). Your can define your multiple environments, which you then select with `graphes --env <env_name>` in the CLI. The content resembles the following:

```yaml
# Default export path for index dumps
export_path: /path/to/data/elasticsearch_exports

# ElasticSearch environments (names must end in '_env')
environments:

  # Core services / test environment
  coresrv_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD

  # Prod environment
  prod_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD

  # Other environments
  something_else_env:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD

# Default environment to use when not specified in the GraphES CLI.
default_env: coresrv_env
```

### Graph AI

The configuration for the GraphAI connection is defined in: 📂 [config_graphai.yml](config_examples/environment/config_graphai.example.yml). The content resembles the following:

```yaml
graphai:
    host_address: HOST_ADDRESS
    port: PORT
    username: USERNAME
    password: PASSWORD
```

### Generative AI [optional]

If you have access to a local or commercial LLM service, which Graph Registry can (optionally) leverage to improve semantic analysis and enrich your raw data, you can configure it in: 📂 [config_genai.yml](config_examples/environment/config_genai.example.yml). The content resembles the following:

```yaml
genai:
    api_key: API_KEY
    inference_url: "https://api.example.com/v1"
    llm_model: "moonshotai/Kimi-K2.7-Code"
```

## Application setup

### Registry API

The configuration for the Graph Registry API is defined in: 📂 [config_api.json](config_examples/application/config_api.example.json). It contains the allowed object types for graph nodes and edges that an API user can insert (more on this in Section #). The content resembles the following:

```json
{
     "allowed-types" : {
        "nodes" : [
            "Course",
            "Lecture",
            "Person",
            "Publication",
            "Unit"
        ],
        "edges" : [
            ["Course", "Person", "teacher"],
            ["Lecture", "Course", "part of"],
            ["Person", "Unit", "accreditation"],
            ["Person", "Unit", "position group ranking"],
            ["Person", "Unit", "position grouping"],
            ["Publication", "Person", "authorship"],
            ["Unit", "Unit", "affiliation"],
            ["Unit", "Unit", "subtype ranking"]
        ]
    }
}
```

### Registry Airflow

The configuration for the Graph Registry airflow mechanism is defined in: 📂 [config_airflow.json](config_examples/application/config_airflow.example.json). It allows you to setup which node and edge object types you want to process on each data refresh cycle (more on this in Section #). The content resembles the following:

```json
{
    "nodes": [
        ["Category", true, true],
        ["Concept", true, true],
        ["Course", true, true],
        ["Lecture", true, true],
        ["Person", true, true],
        ["Publication", true, true],
        ["Unit", true, true]
    ],
    "edges": [
        ["Category", "Category", true],
        ["Category", "Concept", true],
        ["Course", "Lecture", true],
        ["Course", "Person", true],
        ["Person", "Publication", true],
        ["Person", "Unit", true],
        ["Unit", "Unit", true]
    ]
}
```

### Semantic scoring

The configuration for the Graph Registry semantic scoring is defined in: 📂 [config_scores.json](config_examples/application/config_scores.example.json). It allows you to setup which node-to-node tuples you wish to be semantically connected and scored (more on this in Section #). The content resembles the following:

```json
{
    "scored-edge-tuples" : {
        "education" : [
            ["Course", "Course"],
            ["Course", "Lecture"],
            ["Lecture", "Lecture"]
        ],
        "research" : [
            ["Person", "Person"],
            ["Person", "Publication"],
            ["Person", "Unit"],
            ["Publication", "Publication"],
            ["Publication", "Unit"],
            ["Unit", "Unit"]
        ]
    },
    "mixed-scoring-tuples" : [
        ["Category", "Category"],
        ["Course", "Person"],
        ["Person", "Unit"],
        ["Unit", "Unit"]
    ]
}
```

### Indexing setup

The configuration for the Graph Registry indexing setup is defined in: 📂 [config_index.json](config_examples/application/config_index.example.json). It allows you to setup the graph indexing rules for the GraphSearch application and the ElasticSearch index, including which node and edge types to index, which custom fields to include, and how to rank recommendation lists.

The content of this file is too long and complex to display here. You should start with the provided default configuration, and modify it in accordance with your use case as you get more familiar with the format.

Getting Started
===============
Once you set up your environment and application config files, you are ready to execute a full data processing cycle on Graph Registry.

For a test run with the provided sample data, you do not require Graph AI nor Graph Ontology to be deployed. Pre-calculated results and sample sets are provided, allowing you to bypass the deployment of those two services for now.

You can start by testing the CLI as follows:

```shell
graphregistry test
graphregistry -h
```

If everything works as expected, you are good to go! 🚀

> [!TIP]
> You can print the help info for all the CLI commands and sub-commands with `graphregistry <cmd> <subcmd> -h`

## Test your configuration

As a first step, your can validate and check your configuration. The CLI provides the command `config` to execute operations related to your configuration files.

```shell
graphregistry config validate
```

You can also show a pretty-print summary of your configuration files:

```shell
graphregistry config show
```

## Initialize the application environment

If you start from an empty environment, you can run the initialization command to generate all the necessary databases and tables utilized by the Registry application:

```shell
graphregistry setup init
```

This command assumes the [Graph Ontology](https://github.com/epflgraph/graphontology) has already been imported into your database. If not, you can import the ontology sample set as follows:

```shell
graphregistry setup init --import-ontology-sample
```

## Ingesting and managing data

There are two ways of inserting data into the Registry database: using the CLI or the API. To help you get started, a sample dataset is provided in the folder: 📂 [examples/sample_sets](examples/sample_sets).

The command for managing Registry data is:

```shell
graphregistry data
```

For example, you can use the subcommand `save` to insert a list of nodes:

```shell
graphregistry data save --node_list examples/sample_sets/sample_epfl_node_list.json
```

and the corresponding list of edges:

```shell
graphregistry data save --edge_list examples/sample_sets/sample_epfl_edge_list.json
```

Alternatively, you can replicate the exact same operation with the API as follows:

```shell
jq '.' examples/sample_sets/sample_epfl_node_list.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/nodes/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
```

for the same list of nodes, and the same thing for the edges:

```shell
jq '.' examples/sample_sets/sample_epfl_edge_list.json \
| curl -sS -X POST 'http://127.0.0.1:9999/api/edges/save_many' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d @- \
| jq '.'
```

You can also use the Swagger UI interface to experiment with data management using the API:<br />
🌍 [http://127.0.0.1:9999/docs](http://127.0.0.1:9999/docs)

You have an example of how to execute each data management operation, using either the CLI or the API, in the folder: 📂 [examples/entrypoints](examples/entrypoints).
