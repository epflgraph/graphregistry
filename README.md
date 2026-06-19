<img src="assets/icon.png" alt="Project logo" height="64">

[![License](https://img.shields.io/github/license/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/blob/master/LICENSE)
[![Latest Release on Github](https://img.shields.io/github/v/release/epflgraph/graphregistry?sort=semver)](https://github.com/epflgraph/graphregistry/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/epflgraph/graphregistry?style=social)](https://github.com/epflgraph/graphregistry/stargazers)
[![Contributors](https://img.shields.io/github/contributors/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/commits/master)
[![Open Issues](https://img.shields.io/github/issues/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/issues)
[![Open PRs](https://img.shields.io/github/issues-pr/epflgraph/graphregistry)](https://github.com/epflgraph/graphregistry/pulls)

Why Graph?
==========
The *Graph Data Platform* - developed by the AI engineering team at the [EPFL Center for Digital Education](https://www.epfl.ch/education/educational-initiatives/cede/) - is an open-source alternative to proprietary research information systems like Elsevier Pure. It federates educational and institutional data into a semantically interconnected knowledge graph of people, publications, labs, startups, courses, video lectures, and other educational resources. The [GraphSearch](https://graphsearch.epfl.ch/en) application provides lightning-fast search and discovery of the knowledge graph, as well as LLM-powered [chatbot](https://graphsearch.epfl.ch/en/chatbot) interaction with the indexed resources.

**List of Graph services:**<br/>
Registry |
[AI](https://github.com/epflgraph/graphai) |
[Ontology](https://github.com/epflgraph/graphontology) |
[Search](https://github.com/epflgraph/graphsearch_ui) |
[Chat](https://github.com/epflgraph/graphchatbot) |
[Dash](https://github.com/epflgraph/graphdashboard) |
[DB client](https://github.com/epflgraph/graphdb-client) |
[ES client](https://github.com/epflgraph/graphes-client)

Graph Registry
==============
*Graph Registry* is the first layer in the Graph Data Platform. It ingests data in JSON format through an ETL pipeline, and generates a knowledge graph that feeds the GraphSearch and GraphChat applications.

Data can be added to the registry through direct JSON file imports, or through a REST API. The actions steps in the knowledge graph construction are executed through a command line interface (CLI).

Installation
============
1. (OPTIONAL) create a virtual environment with `python -m venv venv` and activate with `source venv/bin/activate`
2. install the requirements with `pip install -r requirements.txt`
3. you will also need to install vlc with `sudo apt install vlc`

Configuration
=============
Copy the example_config.yaml file to config.yaml and edit it to give your graphai, elasticsearch and mysql credentials.

You may also need to copy the certificates for connecting to elasticsearch (by default in `resources/certificates/`).

You may also need to create the graphai-client JSON configuration file and give its location in the 
`graphai.client_config_file` section of your `config.yaml` file. It should look like this:

```json
{
  "host": "https://graphai.epfl.ch",
  "port": 443,
  "user": "YOUR_GRAPHAI_USERNAME",
  "password": "YOUR_GRAPHAI_PASSWORD"
}
```


RUN
======
From the package root directory run:

```uvicorn graphregistry.entrypoints.api.main:create_app --reload --factory```

TEST
=======
Example queries are available in the `resources/api_request_examples/` directory for testing the API. 
Refer to the README.md file there for further information.


<!--
# set up password for es:

# using the Docker DNS name
docker compose exec elasticsearch bin/elasticsearch-reset-password -u elastic -i --url https://elasticsearch:9200

curl --cacert ./.certs/ca.crt -u elastic:$NEWPASS https://127.0.0.1:9200/_cluster/health?pretty


kibana:

docker compose exec elasticsearch bin/elasticsearch-service-tokens create elastic/kibana kibana-token

[copy token to .env in var KIBANA_SERVICE_TOKEN]

restart: docker compose up -d kibana
-->