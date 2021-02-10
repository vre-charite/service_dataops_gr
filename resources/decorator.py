from flask_jwt import current_identity
import requests
from config import ConfigClass
from functools import wraps
import json
from flask import request
from services.logger_services.logger_factory_service import SrvLoggerFactory
from models.user_type import EUserRole, map_role_front_to_sys, map_role_neo4j_to_sys
from services.user_services.user_authorization import user_accessible

_logger = SrvLoggerFactory('check_role').get_logger()

def check_role(required_role, parent=None):
    def inner_function(function):
        required_role_mapped = map_role_front_to_sys(required_role)

        @wraps(function)
        def wrapper(*args, **kwargs):

            user_id = current_identity["user_id"]
            role = current_identity["role"]
            role_mapped = map_role_front_to_sys(role)
            #########################################
            # note here this admin is platform wise #
            # and require_role is project wise      #
            #########################################
            # check if user is platform admin
            if(role_mapped == EUserRole.admin):
                current_identity['project_role'] = 'admin'
                res = function(*args, **kwargs)
                return res

            # required role is site admin
            if required_role_mapped == EUserRole.site_admin:
                return {'result': 'Permission Denied'}, 401

            if(parent):
                dataset_id = request.get_json().get("parent_id")
            else:
                dataset_id = kwargs.get('container_id', None)
                if dataset_id == None:
                    dataset_id = request.args.get('container_id')

            # check if the relation is existed in neo4j
            try:
                url = ConfigClass.NEO4J_SERVICE + "relations"
                url += "?start_id=%d" % int(user_id)
                url += "&end_id=%d" % int(dataset_id)
                res = requests.get(url=url)
                _logger.debug('check if the relation is existed in neo4j: ' + url)
                if(res.status_code != 200):
                    raise Exception("Unauthorized: " +
                                    json.loads(res.text))
                relations = json.loads(res.text)
                if(len(relations) == 0):
                    raise Exception(
                        "Unauthorized: Relation does not exist.")
            except Exception as e:
                return {'result': 'Permission Denied'}, 401
            
            try:
                for item in relations:
                    r = item["r"]["type"]
                    role_neo4j_mapped = map_role_neo4j_to_sys(r)
                    current_identity['project_role'] = r
                    if(user_accessible(required_role_mapped, role_neo4j_mapped)):
                        # if user accessible pass authorization and continue function
                        res = function(*args, **kwargs)
                        return res
            except Exception as e:
                _logger.error('Role Auth Failed: ' + str(e) + '----------------r: {}, role_neo4j_mapped: {}'.format(r, role_neo4j_mapped))
                return {'result': 'Role Auth Failed: ' + str(e)}, 401

            # if not pass the authorization
            return {'result': 'Permission Denied'}, 401
        return wrapper
    return inner_function


def check_user():
    def inner_function(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            current_user = current_identity["username"]
            username = kwargs['username']

            if current_user != username:
                return {'result': 'Permission Denied'}, 401

            res = function(*args, **kwargs)
            return res

        return wrapper
    return inner_function


def check_folder_permissions(function):
    def wrapper(*args, **kwargs):
        folder_id = kwargs.get('folder_id', None)
        url = ConfigClass.NEO4J_SERVICE + "relations"
        params = {"start_id": current_identity["user_id"], "end_id": folder_id}
        res = requests.get(url=url, params=params)
        if(res.status_code != 200):
            raise Exception("Unauthorized: " + json.loads(res.text))
        relations = json.loads(res.text)
        print(relations)
        if relations[0]['r']['type'] == 'owner':
            return function(*args, **kwargs)
        else:
            raise Exception("Unauthorized: " + json.loads(res.text))
    return wrapper

