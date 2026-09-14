# Installation

GraphRegistry can be deployed with Docker or installed as a local Python package.

## Prerequisites

- Python 3.10+ (for local installation)
- Docker and Docker Compose (for containerised deployment)
- A running MySQL/MariaDB instance
- A running Elasticsearch instance (for indexing)
- Access to GraphAI (optional, for concept detection and media processing)

## Deploy with Docker

The Docker image packages the API and CLI together. Mount your `config/` directory read-only so the container can read the required configuration files.

```yaml title="docker-compose.yml"
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

Start the service:

```bash
docker compose up -d
```

Run a CLI command inside the container:

```bash
docker compose exec graphregistry graphregistry -h
```

For convenience, you can alias the container command in your shell:

```bash title="~/.zshrc"
graphregistry() {
  docker compose exec graphregistry graphregistry "$@"
}
```

## Local installation

1. Clone the repository:

    ```bash
    git clone https://github.com/epflgraph/graphregistry.git
    cd graphregistry
    ```

2. Create and activate a virtual environment:

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. Install the package:

    ```bash
    pip install .
    ```

4. Verify the CLI:

    ```bash
    graphregistry -h
    graphregistry test
    ```

5. Start the API:

    ```bash
    exec uvicorn graphregistry.entrypoints.api.main:create_app \
      --host 127.0.0.1 \
      --port 9999 \
      --workers 1 \
      --proxy-headers \
      --forwarded-allow-ips 127.0.0.1 \
      --factory
    ```

The Swagger UI is then available at [http://127.0.0.1:9999/docs](http://127.0.0.1:9999/docs).

## Installing Material for MkDocs

To build this documentation locally:

```bash
pip install mkdocs-material
mkdocs serve
```

For the optional minify plugin:

```bash
pip install mkdocs-minify-plugin
```
