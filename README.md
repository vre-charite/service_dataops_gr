# Data Operation Service

## Getting Started

- Prerequisites
  - Python 3.7.3
  - You can find all prerequisites in  [requirements.txt](https://git.indocresearch.org/platform/service_user_management/blob/master/requirements.txt), install all by `pip install -r requirements.txt`
- Start with python

```bash
$ python app.py
```

- Start with gunicorn

```bash
$ ./gunicorn_starter.sh
```

- Start with Docker, run on port 5063

```bash
$ sudo docker-compose up
```

- Notice that the service is depended on another service [Neo4j Service](https://git.indocresearch.org/platform/dataset_neo4j).



## Folder Structure

```
.
├── Dockerfile 
├── README.md
├── access.log # gunicorn generated access log
├── app
│   └── __init__.py
├── app.py
├── config.py
├── docker-compose.yml
├── error.log # gunicorn generated error log
├── gunicorn_config.py
├── gunicorn_starter.sh
├── nfs_ops # nfs operation apis
│   ├── __init__.py
│   ├── file_api.py
│   └── folder_api.py
├── requirements.txt
└── resources
    └── utils.py # helper functions
```



## Service

All APIs will be documented in [Dockerize data operation service](https://indocconsortium.atlassian.net/browse/VRE-181)

