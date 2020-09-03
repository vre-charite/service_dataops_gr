from flask_jwt import current_identity
import requests
from config import ConfigClass
from functools import wraps
import json
from flask import request


def check_role(required_role, parent=None):
    def inner_function(function):
        roles = {
            "admin": 3,  # highest permission
            "member": 2,
            "uploader": 1  # lowest permission
        }

        @wraps(function)
        def wrapper(*args, **kwargs):
            print("###### check_role")

            user_id = current_identity["user_id"]
            role = current_identity["role"]
            # check if user is platform admin
            if(role == "admin"):
                current_identity['project_role'] = 'admin'
                res = function(*args, **kwargs)
                return res

            # required role is site admin
            if required_role == "site-admin":
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
                if(res.status_code != 200):
                    raise Exception("Unauthorized: " +
                                    json.loads(res.text))
                relations = json.loads(res.text)
                if(len(relations) == 0):
                    raise Exception(
                        "Unauthorized: Relation does not exist.")
            except Exception as e:
                return {'result': 'Permission Denied'}, 401

            for item in relations:
                r = item["r"]["type"]
                if(roles[r] >= roles[required_role]):
                    # also save the role to current project
                    current_identity['project_role'] = r
                    # if current role is not lower than requried role
                    # pass authorization and continue function
                    res = function(*args, **kwargs)
                    return res

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
