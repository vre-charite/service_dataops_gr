from transitions import Machine
from enum import Enum

class EState(Enum):
    INIT = 0,
    PRE_UPLOADED = 1,
    CHUNK_UPLOADED = 2,
    FINALIZED = 3,
    SUCCEED = 4,
    TERMINATED = 5

class ETrigger(Enum):
    pre_process = 0,
    chunk_upload = 1,
    finalize = 2,
    complete = 3,
    terminate = 4

class Model(object):
    pass

file_upload_sm_model = Model()

fsm_file_upload_states = [
    EState.INIT.name,
    EState.PRE_UPLOADED.name,
    EState.CHUNK_UPLOADED.name,
    EState.FINALIZED.name,
    EState.SUCCEED.name,
    EState.TERMINATED.name,
]

# transistion rule
fsm_file_upload_transitions = [
    {'trigger': ETrigger.pre_process.name, 'source': EState.INIT.name, 'dest': EState.PRE_UPLOADED.name},
    {'trigger': ETrigger.chunk_upload.name, 'source': EState.PRE_UPLOADED.name, 'dest': EState.CHUNK_UPLOADED.name },
    {'trigger': ETrigger.finalize.name, 'source': EState.CHUNK_UPLOADED.name, 'dest': EState.FINALIZED.name },
    {'trigger': ETrigger.complete.name, 'source': EState.FINALIZED.name, 'dest': EState.SUCCEED.name },
    # termination
    {'trigger': ETrigger.terminate.name, 'source': EState.INIT.name, 'dest': EState.TERMINATED.name},
    {'trigger': ETrigger.terminate.name, 'source': EState.PRE_UPLOADED.name, 'dest': EState.TERMINATED.name },
    {'trigger': ETrigger.terminate.name, 'source': EState.CHUNK_UPLOADED.name, 'dest': EState.TERMINATED.name },
    {'trigger': ETrigger.terminate.name, 'source': EState.FINALIZED.name, 'dest': EState.TERMINATED.name },
]

# factory a state machine for file upload
def factory_fsm_file_upload(init_state = EState.INIT.name):
    machine = Machine(
        model=file_upload_sm_model,
        states=fsm_file_upload_states,
        transitions=fsm_file_upload_transitions,
        initial=init_state)
    return machine
