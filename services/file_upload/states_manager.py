import models.fsm_file_upload as fsmfu
import datetime, json
from models.service_meta_class import MetaService
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.data_providers.redis import SrvRedisSingleton
from services.neo4j_services.container_manager import SrvContainerManager
from config import ConfigClass
from flask_jwt import current_identity

_logger = SrvLoggerFactory('api_file_upload').get_logger()

class SrvFileUpStateMgr(metaclass=MetaService):
    def __init__(self, session_id, container_id, task_id, file_name=None):
        _logger.debug(session_id)
        srv_container_mgr = SrvContainerManager()
        project_code = srv_container_mgr.fetch_container_by_id(container_id)[0]['code']
        _logger.debug('project_code {}'.format(str(project_code)))
        self.srv_redis = SrvRedisSingleton()
        self.__instance = upload_task_init_factory(
            session_id,
            container_id,
            project_code,
            task_id)
        db_record = self.check_db()
        if db_record:
            json_str = db_record.decode("utf-8")
            model_dict = json.loads(json_str)
            self.__instance.task_id = model_dict['task_id']
            self.__instance.state = model_dict['state']
            self.machine = fsmfu.factory_fsm_file_upload(self.__instance.state)
            self.__instance.session_id = session_id
            self.__instance.start_timestamp = model_dict['start_timestamp']
            self.__instance.end_timestamp = model_dict['end_timestamp']
            self.__instance.file_name = model_dict['file_name']
        else:
            self.__instance.task_id = task_id
            self.__instance.state = fsmfu.EState.INIT.name
            self.machine = fsmfu.factory_fsm_file_upload(self.__instance.state)
            self.__instance.session_id = session_id
            self.__instance.start_timestamp = None
            self.__instance.end_timestamp = None
            self.__instance.file_name = file_name

    def go(self, target_state: fsmfu.EState):
        old_state = self.__instance.state
        if self.can_go(target_state):
            self.machine.set_state(target_state.name)
            self.__instance.state = target_state.name
            self.__instance.frontend_state = self.frontend_state
            self.on_state_changed(target_state)
            _logger.debug('{} status change from {} to {}'.format(
                self.__instance.key,
                old_state,
                self.__instance.state))
        else:
            raise(Exception(
                'Can not transit state from {} to {}'.format(
                    self.__instance.state, target_state.name)))

    def can_go(self, target_state: fsmfu.EState):
        __state = target_state.name
        rule = [transition for transition in fsmfu.fsm_file_upload_transitions
            if transition['source'] == self.__instance.state
            and transition['dest'] == __state]
        return True if rule else False

    @property
    def frontend_state(self):
        return {
            '{}'.format(fsmfu.EState.INIT.name): 'uploading',
            '{}'.format(fsmfu.EState.PRE_UPLOADED.name): 'uploading',
            '{}'.format(fsmfu.EState.CHUNK_UPLOADED.name): 'finalizing',
            '{}'.format(fsmfu.EState.FINALIZED.name): 'finalized',
            '{}'.format(fsmfu.EState.SUCCEED.name): 'succeed',
            '{}'.format(fsmfu.EState.TERMINATED.name): 'terminated',
        }.get(
            self.__instance.state,
            'unknown'
        )

    @property
    def current_state(self):
        return self.__instance.state
    
    def check_db(self):
        return self.srv_redis.get_by_key(self.__instance.key)

    def update_db(self):
        _logger.debug(self.__instance.key)
        self.srv_redis.set_by_key(self.__instance.key, json.dumps(self.__instance.to_dict))

    def on_state_changed(self, new_state: fsmfu.EState):
        if new_state == fsmfu.EState.PRE_UPLOADED:
            ## init upload job settings
            self.__instance.start_timestamp = datetime.datetime.utcnow().isoformat()
        if new_state == fsmfu.EState.SUCCEED:
            self.__instance.end_timestamp = datetime.datetime.utcnow().isoformat()
        if new_state == fsmfu.EState.TERMINATED:
            self.__instance.end_timestamp = datetime.datetime.utcnow().isoformat()
        self.update_db()
        _logger.debug(get_by_session_id(self.__instance.session_id))

class TaskModel():
    def __init__(self):
        self._attribute_map =  {
            "key": "",
            "session_id": "",
            "task_id": "",
            "start_timestamp": None,
            "end_timestamp": None,
            "frontend_state": "uploading",
            "state": fsmfu.EState.INIT.name,
            "progress": 0.0,
            "file_name": "",
            "project_code": "",
            "project_id": "",
        }
    @property
    def to_dict(self):
        return self._attribute_map
    @property
    def key(self):
        return self._attribute_map['key']
    @key.setter
    def key(self, key):
        self._attribute_map['key'] = key
    @property
    def session_id(self):
        return self._attribute_map['session_id']
    @session_id.setter
    def session_id(self, session_id):
        self._attribute_map['session_id'] = session_id
    @property
    def task_id(self):
        return self._attribute_map['task_id']
    @task_id.setter
    def task_id(self, task_id):
        self._attribute_map['task_id'] = task_id
    @property
    def start_timestamp(self):
        return self._attribute_map['start_timestamp']
    @start_timestamp.setter
    def start_timestamp(self, start_timestamp):
        self._attribute_map['start_timestamp'] = start_timestamp
    @property
    def end_timestamp(self):
        return self._attribute_map['end_timestamp']
    @end_timestamp.setter
    def end_timestamp(self, end_timestamp):
        self._attribute_map['end_timestamp'] = end_timestamp
    @property
    def frontend_state(self):
        return self._attribute_map['frontend_state']
    @frontend_state.setter
    def frontend_state(self, frontend_state):
        self._attribute_map['frontend_state'] = frontend_state
    @property
    def state(self):
        return self._attribute_map['state']
    @state.setter
    def state(self, state: str):
        self._attribute_map['state'] = state
    @property
    def progress(self):
        return self._attribute_map['progress']
    @progress.setter
    def progress(self, progress):
        self._attribute_map['progress'] = progress
    @property
    def file_name(self):
        return self._attribute_map['file_name']
    @file_name.setter
    def file_name(self, file_name):
        self._attribute_map['file_name'] = file_name
    @property
    def project_code(self):
        return self._attribute_map['project_code']
    @project_code.setter
    def project_code(self, project_code):
        self._attribute_map['project_code'] = project_code
    @property
    def project_id(self):
        return self._attribute_map['project_id']
    @project_id.setter
    def project_id(self, project_id):
        self._attribute_map['project_id'] = project_id
        
def upload_task_init_factory(session_id, container_id, project_code, task_id):
    '''
    redis key = 'session_id:container_id:task_id'
    '''
    my_model = TaskModel()
    my_model.project_id = container_id
    my_model.session_id = session_id
    my_model.task_id = task_id
    my_model.project_code = project_code
    my_model.key = '{}:{}:{}'.format(session_id, container_id, task_id)
    return my_model

def get_by_session_id(session_id):
    srv_redis = SrvRedisSingleton()
    res_binary = srv_redis.mget_by_prefix(session_id)
    return [record.decode('utf-8') for record in res_binary] if res_binary else []

def delete_by_session_id(session_id):
    srv_redis = SrvRedisSingleton()
    srv_redis.delete_by_key(session_id)
    return True

def session_id_generator():
    today = datetime.date.today()
    return 'UPLOAD{}{}'.format(current_identity['user_id'], today.strftime("%d%m%Y"))

def session_id_generator_by_timestamp(timestamp: str):
    '''
    date format '2012-03-01T10:00:00Z'
    '''
    my_date = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    return 'UPLOAD{}{}'.format(current_identity['user_id'], my_date.strftime("%d%m%Y"))