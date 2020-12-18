from flask_restx import Api, Resource, fields
from models.api_meta_class import MetaAPI
from flask import request
import json
from .namespace import api_file_actions_ns
from .transfer import FileTransferRestful
from .queue_transfer import FileTransferQueueRestful
from .file_actions_status import FileActionsStatus
from .actions_query import ActionsQueryRestful

## refactoring upload
class APIFileTransfer(metaclass=MetaAPI):
    def api_registry(self):
        api_file_actions_ns.add_resource(
            FileTransferRestful, '/file/actions/transfer'
        )
        api_file_actions_ns.add_resource(
            FileTransferQueueRestful, '/file/actions/transfer-to-core'
        )
        api_file_actions_ns.add_resource(
            FileActionsStatus, '/file/actions/status'
        )
        api_file_actions_ns.add_resource(
            ActionsQueryRestful, '/file/actions/logs'
        )