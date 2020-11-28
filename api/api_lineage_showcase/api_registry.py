from flask_restx import Api, Resource, fields
from resources.decorator import check_role
from config import ConfigClass
from models.api_meta_class import MetaAPI
from flask import request
import json
from .namespace import api_lineage_showcase_ns
from .lineage_showcase_testpipeline import LineageShowcaseRestful

## refactoring upload
class APILineageShowcase(metaclass=MetaAPI):
    def api_registry(self):
        api_lineage_showcase_ns.add_resource(
           LineageShowcaseRestful, '/lineage-showcase'
        )