# gunicorn_config.py
import multiprocessing

preload_app = True
bind = '0.0.0.0:5063'
daemon = 'false'
# worker config
workers = multiprocessing.cpu_count() * 2 + 1
threads = multiprocessing.cpu_count() * 2
worker_connections = 1200
accesslog = 'gunicorn_access.log'
errorlog = 'gunicorn_error.log'
loglevel = 'debug'
