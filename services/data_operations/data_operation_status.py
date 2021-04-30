from services.data_providers.redis import SrvRedisSingleton
import os, json, time, enum

def set_status(session_id, job_id, source, action, target_status,
    project_code, operator, progress, payload={}):
    srv_redis = SrvRedisSingleton()
    my_key = "dataaction:{}:{}:{}:{}:{}:{}".format(session_id, job_id, action, project_code, operator, source)
    my_value = json.dumps({
        "session_id": session_id,
        "job_id": job_id,
        "source": source,
        "action": action,
        "status": target_status,
        "project_code": project_code,
        "operator": operator,
        "progress": progress,
        'update_timestamp': str(round(time.time())),
        'payload': payload
    })
    srv_redis.set_by_key(my_key, my_value)

def get_status(session_id, job_id, project_code, action, operator = None):
    srv_redis = SrvRedisSingleton()
    my_key = "dataaction:{}:{}:{}:{}:*".format(session_id, job_id, action, project_code) if job_id else \
        "dataaction:{}:*:{}:{}:*".format(session_id, action, project_code) 
    res_binary = srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []


class EDataActionType(enum.Enum):
    data_transfer = 0