import os
import subprocess
import time
from config import ConfigClass
import requests
import enum

def transfer_file(_logger, input_path, output_path):
    try:
        output_dir = os.path.dirname(output_path)
        output_file_name = os.path.basename(output_path)
        if os.path.exists(output_path):
            os.remove(output_path)
            _logger.info('remove existed output file: {}'.format(output_path))

        _logger.info('start transfer file {} to {}'.format(input_path, output_path))

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            _logger.info('creating output directory: {}'.format(output_dir))

        if os.path.isdir(input_path):
            _logger.info('starting to copy directory: {}'.format(input_path))
        else:
            _logger.info('starting to copy file: {}'.format(input_path))
        subprocess.call(['rsync', '-avz', '--min-size=1', input_path, output_path])
        # shutil.copyfile(input_path, output_path)
        # store_file_meta_data(output_path, output_file_name, input_path, pipeline_name)
        # create_lineage(input_path, output_path, 'testpipeline', pipeline_name, 'test pipeline', datetime.datetime.utcnow().isoformat())
        _logger.info('Successfully copied file from {} to {}'.format(input_path, output_path))
    except Exception as e:
        _logger.error('Failed to copy file from {} to {}\n {}'.format(input_path, output_path, str(e)))

def transfer_file_message(_logger, session_id, job_id, input_path, project_code,
    generate_id, uploader, operator, operation_type: int):
    my_generate_id = generate_id if generate_id else 'undefined'
    file_name = os.path.basename(input_path)
    output_dest = interpret_operation_location(operation_type, file_name, project_code)
    payload = {
        "event_type": "file_copy",
        "payload": {
            "session_id": session_id,
            "job_id": job_id,
            "input_path": input_path,
            "output_path": output_dest,
            "operation_type": operation_type,
            "project": project_code,
            "generate_id": my_generate_id,
            "uploader": uploader,
            "generic": True,
            "operator": operator
        },
        "create_timestamp": time.time()
    }
    url = ConfigClass.service_queue_send_msg_url
    _logger.info("Sending Message To Queue: " + str(payload))
    res = requests.post(
        url=url,
        json=payload,
        headers={"Content-type": "application/json; charset=utf-8"}
    )
    return res.status_code == 200

class EOperationType(enum.Enum):
    A=0 ## copy to greenroom RAW, straight copy ConfigClass.data_lake + '/' + self.project + '/processed/' + file_name
    B=1 ## copy to vre core RAW, publish data to vre ConfigClass.vre_data_storage + '/' + self.project + '/raw/' + filename

def interpret_operation_location(operation_type: int, file_name, project):
    def to_greenroom_processed(file_name, project):
        return ConfigClass.NFS_ROOT_PATH + '/' + project + '/processed/straight_copy/' + file_name
    def to_vre_core(file_name, project):
        return ConfigClass.VRE_ROOT_PATH + '/' + project + '/raw/' + file_name
    output_generator = {
        EOperationType.A.value: to_greenroom_processed,
        EOperationType.B.value: to_vre_core
    }.get(operation_type)
    return output_generator(file_name, project)