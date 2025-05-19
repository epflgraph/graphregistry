INSTALL
=======
1. (OPTIONAL) create a virtual environment with `python -m venv venv` and activate with `source venv/bin/activate`
2. install the requirements with `pip install -r requirements.txt`
3. you will also need to install vlc with `sudo apt install vlc`

CONFIGURE
===========
Copy the example_config.yaml file to config.yaml and edit it to give your graphai, elasticsearch and mysql credentials.
You may also need to copy the certificates for connecting to elasticsearch (by default in `resources/certificates/`).

RUN
======
From the package root directory run:

```uvicorn api.main.main:app --reload```

TEST
=======
Example queries are available in the `resources/api_request_examples/` directory for testing the API. 
Refer to the README.md file there for further information.
