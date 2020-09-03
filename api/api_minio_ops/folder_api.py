from flask import request
from flask_restful import Resource
from resources.utils import fs
from urllib.parse import unquote
import os
from config import ConfigClass
import requests
import json
from flask_restx import Api, Resource
from api import minio_entity_ns
from resources.swagger_modules import folder, success_return, folder_return
from . import minioClient

class folders(Resource):
    
    @minio_entity_ns.expect(folder)
    @minio_entity_ns.response(200, success_return)
    def post(self):
        '''
        This method allow to create a folder as path in args
        '''
        try:
            # Get folder path from args
            post_data = request.get_json()
            container_id = post_data.get('container_id', None)
            path = post_data.get('path', None)

            # we got param for specific then go to neo4j get path and create subfolder
            if container_id:
                url = ConfigClass.NEO4J_SERVICE + \
                    "nodes/Dataset/node/" + str(container_id)
                res = requests.get(url=url)
                datasets = json.loads(res.text)
                if(res.status_code != 200):
                    return {'result': json.loads(res.text)}, res.status_code
                if(len(datasets) == 0):
                    return {'result': "Container is not exist."}, 403
                bucket = datasets[0]['path']

                # the use the sub path in payload to make the object
                # since the minio cannot create an empty bucket so I will give an sample file
                res = minioClient.fput_object(bucket, path+'/init.txt', "./resources/init.txt")

            # if else we create the top level bucket
            else:
                minioClient.make_bucket(path)


        except Exception as e:
            return {'Error': str(e)}, 403

        return {'result': "success"}, 200

    @minio_entity_ns.doc(params = {'field': {'type': 'string'}})
    @minio_entity_ns.expect(folder)
    @minio_entity_ns.response(200, folder_return)
    def get(self):
        '''
        This method allow to walk through folders.
        '''
        try:
            # Get folder path from args, decode utf-8
            container_id = request.args.get('container_id', None)
            if not container_id:
                return {'result': "container_id is required."}, 403


            result = []
            # bukcets = []

            # we got param for specific then go to neo4j get path
            url = ConfigClass.NEO4J_SERVICE + \
                "nodes/Dataset/node/" + str(container_id)
            res = requests.get(url=url)
            datasets = json.loads(res.text)
            if(res.status_code != 200):
                return {'result': json.loads(res.text)}, res.status_code
            if(len(datasets) == 0):
                return {'result': "Container is not exist."}, 403
            bucket = datasets[0]['path']

            objs = minioClient.list_objects(bucket, recursive=True)


            result = {}
            for x in objs:
                current_pointer = result
                folder_file = x.object_name.split('/')
                # print(folder_file)

                # loop over the / split make into json
                # the last on are always the file
                for index in range(0, len(folder_file)-1):
                    temp = current_pointer.get(folder_file[index], None)
                    if not temp:
                        current_pointer.update({folder_file[index]:{}})
                    current_pointer = current_pointer.get(folder_file[index])

                # at the end we add the last which is the file
                if len(folder_file)-1 > 0:
                    current_pointer.update({folder_file[index]:folder_file[index+1]})
                else:
                    # print(index)
                    current_pointer.update({folder_file[index]:[]})

            # print(result)

            def recursive_format(json_object):
                ret = []
                for key, value in json_object.items():
                    # print(key, value)
                    if isinstance(value, dict):
                        temp = recursive_format(value)
                        ret.append({key: temp})
                    elif isinstance(value, list):
                        ret.append(key)
                    else:
                        ret.append(value)

                return ret


        except Exception as e:
            return {'Error': str(e)}, 500

        return {'result': recursive_format(result)}, 200
