from config import ConfigClass
import os

def check_is_greenroom_raw(full_path, project_code):
    greenroom_raw_folder = ConfigClass.NFS_ROOT_PATH + '/' + project_code + '/raw'
    return os.path.dirname(full_path) == greenroom_raw_folder

def check_is_vre_core_raw(full_path, project_code):
    vre_core_raw_folder = ConfigClass.VRE_ROOT_PATH + '/' + project_code + '/raw'
    return os.path.dirname(full_path) == vre_core_raw_folder