from flask_restx import Api, Resource, fields
from api import module_api


file_upload = module_api.model("file_upload", {
    'file': fields.Raw,
    "resumableChunkNumber": fields.Integer,
    "resumableChunkSize": fields.Integer,
    "resumableCurrentChunkSizef": fields.Integer,
    "resumableTotalSize": fields.Integer,
    "resumableType": fields.String,
    "resumableIdentifier": fields.String,
    "resumableFilename": fields.String,
    "resumableRelativePath": fields.String,
    'resumableTotalChunks': fields.Integer,
    'generateID': fields.Integer,
    'uploader': fields.String,
    "chunk": fields.Integer,
    # "subPath": fields.String
    # "tags": fields.Array
})

folder = module_api.model("folder", {
    "path": fields.String
})

file_download = module_api.model("file_download", {
    "path": fields.String,
    "filename": fields.String
})

file_upload_last_response = '''
    {
        result: "All chunks received, task_id is upload0d3ad90d-7677-41c7-b940-e39e7eda8ab1"
    }
'''

file_upload_status = '''
    {
        "result": "task does not exist"
    }
'''

success_return = '''
    {
        result: success
    }
'''

folder_return = '''
    {
        "result": [
            {
                "folder1": [
                    {
                        "folder11": []
                    },
                    {
                        "folder12": [
                            "Battlefield™ 1 2020_5_14 17_18_20.png"
                        ]
                    },
                    "hypre-v.txt",
                    "Battlefield™ 1 2020_5_14 17_18_20.png",
                    "CityofToronto_COVID-19_Data.xlsx"
                ]
            },
            "Battlefield™ 1 2020_5_14 17_18_20.png"
        ]
    }
'''
