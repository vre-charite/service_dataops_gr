# gunicorn_config.py
from resources.number_factoring import factoring_best_combo
import multiprocessing
from services.logger_services.logger_factory_service import SrvLoggerFactory

gunicorn_config_logger = SrvLoggerFactory('gunicorn').get_logger()
cpu_performance_value = multiprocessing.cpu_count() * 2 + 1
gunicorn_config_logger.info('cpu_performance_value: ' + str(cpu_performance_value))
best_combo = factoring_best_combo(cpu_performance_value)

preload_app = True
bind = '0.0.0.0:5063'
daemon = 'false'
# worker config
workers = best_combo[0]
threads = best_combo[1]
worker_connections = 1200
accesslog = 'gunicorn_access.log'
errorlog = 'gunicorn_error.log'
loglevel = 'debug'

gunicorn_config_logger.info("Gunicorn Workers: " + str(workers))
gunicorn_config_logger.info("Gunicorn Threads: " + str(threads))
