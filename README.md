INSTALL
=======
1. (OPTIONAL) create a virtual environment with `python -m venv venv` and activate with `source venv/bin/activate`
2. install the requirements with `pip install -r requirements.txt`
3. you will also need to install vlc with `sudo apt install vlc`

CONFIGURE
===========
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

```uvicorn api.main.main:app --reload```

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