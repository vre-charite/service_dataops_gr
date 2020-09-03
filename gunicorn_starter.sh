#!/bin/sh

gunicorn -c gunicorn_config.py "app:create_app()"
