#!/bin/sh

gunicorn -c gunicorn_config.py "run:flaskapp"
