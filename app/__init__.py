from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
import requests
from flask_cors import CORS
from config import ConfigClass
from flask_executor import Executor
import importlib
import json
import multiprocessing

from flask_jwt import JWT,  JWTError
import jwt as pyjwt

import os
from services.logger_services.logger_factory_service import SrvLoggerFactory

executor = Executor()

def create_db(app):
    db = SQLAlchemy(app)
    return db

def create_app(extra_config_settings={}):
    # initialize app and config app
    app = Flask(__name__)
    app.config.from_object(__name__+'.ConfigClass')
    app_logger = SrvLoggerFactory("main").get_logger()
    app.logger = app_logger
    CORS(
        app,
        origins="*",
        allow_headers=["Content-Type", "Authorization",
                       "Access-Control-Allow-Credentials"],
        supports_credentials=True,
        intercept_exceptions=False)

    # initialize flask executor
    executor.init_app(app)

    # enable JWT
    jwt = JWT(app)

    @jwt.jwt_error_handler
    def error_handler(e):
        print("###### Error Handler")
        # Either not Authorized or Expired
        app_logger.info('Found Token Error: ' + str(e))
        return {'result': 'jwt ' + str(e)}, 401

    # load jwt token from request's header
    @jwt.request_handler
    def load_token():
        print("###### Load Token")
        token = request.headers.get('Authorization')
        if not token:
            return token

        return token.split()[-1]

    # function is to parse out the infomation in the JWT
    @jwt.jwt_decode_handler
    def decode_auth_token(token):
        print("###### decode_auth_token by syncope")
        try:
            decoded = pyjwt.decode(token, verify=False)
            return decoded
        except Exception as e:
            raise JWTError(description='Error', error=e)

    # finally we pass the infomation to here to identify the user
    @jwt.identity_handler
    def identify(payload):
        print("###### identify")
        username = payload.get('preferred_username', None)

        # check if preferred_username is encoded in token
        if(not username):
            raise Exception("preferred_username is required in jwt token.")

        try:
            # check if user is existed in neo4j
            url = ConfigClass.NEO4J_SERVICE + "nodes/User/query"
            res = requests.post(
                url=url,
                json={"name": username}
            )
            if(res.status_code != 200):
                raise Exception("Neo4j service: " + json.loads(res.text))
            users = json.loads(res.text)
            if(len(users) == 0):
                raise Exception(
                    "Neo4j service: User %s does not exist." % username)
            user_id = users[0]['id']
            role = users[0]['role']

        except Exception as e:
            raise JWTError(description='Error', error=e)

        return {"user_id": user_id, "username": username, "role": role}

    # dynamic add the dataset module by the config we set
    for apis in ConfigClass.api_modules:
        api = importlib.import_module(apis)
        api.module_api.init_app(app)

    # initialize logging
    if not os.path.exists('./logs'):
        os.makedirs('./logs')

    app_logger.info('Start flask application')

    return app


