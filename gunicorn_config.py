# gunicorn_config.py
workers = 4
threads = 2
bind = '0.0.0.0:5063'
daemon = 'false'
worker_connections = 5
accesslog = 'gunicorn_access.log'
errorlog = 'gunicorn_error.log'
loglevel = 'debug'
