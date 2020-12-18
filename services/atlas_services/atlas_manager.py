import requests
import json
from models.service_meta_class import MetaService
from config import ConfigClass
from datetime import datetime, timedelta, timezone


class SrvAtlasManager(metaclass=MetaService):
    def __init__(self):
        self.url = ConfigClass.METADATA_API

    def query_file_meta(self, container_id, filter_condition, page_size, page, sorting, order, entity_type, container_role):

        # based on the filtering condition
        # loop over the json to add the equal constaint
        # TODO might add the range condition
        criterion = []
        for x in filter_condition:
            if x == 'tags':
                if filter_condition['tags']:
                    tags_criterion = []  # nested tags filter
                    for tag in filter_condition['tags']:
                        tags_criterion.append({
                            'attributeName': '__labels',
                            'attributeValue': tag,
                            'operator': 'contains'
                        })
                    criterion.append({
                        'condition': 'OR',
                        'criterion': tags_criterion
                    })
            elif x == 'bucketName':
                criterion.append({
                    'attributeName': x,
                    'attributeValue': filter_condition[x],
                    'operator': 'eq'
                })
            elif container_role != 'admin' and x == 'owner':
                criterion.append({
                    'attributeName': 'owner',
                    'attributeValue': filter_condition['owner'],
                    'operator': 'eq'
                })
            else:
                criterion.append({
                    'attributeName': x,
                    'attributeValue': filter_condition[x],
                    'operator': 'contains'
                })

        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                "condition": "AND",
                "criterion": criterion
            },
            'tagFilters': None,
            'attributes': ['generateID', 'fileName', 'fileSize', 'path'],
            'limit': page_size,
            'offset': page * page_size,
            'sortBy': sorting,
            'sortOrder': order,
            'typeName': entity_type,
            'classification': None,
            'termName': None
        }
        res = requests.post(self.url+'/v1/entity/basic',
                            json=post_data, headers={'content-type': 'application/json'})
        if res.status_code != 200:
            raise Exception("Failed to call altas service.")
        res = res.json()['result']

        # because if there is no file it will not return entities field
        # so check it up
        if not res.get('entities', None):
            res.update({'entities': []})

        # also change the timestamp from int to string
        for e in res['entities']:
            timestamp_int = e['attributes'].get('createTime', None)
            # print(timestamp_int)
            if timestamp_int:
                central = datetime.fromtimestamp(timestamp_int,tz=timezone.utc)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        return res

    def update_file_label(self, guid, taglist):
        url = '{}/v1/entity/guid/{}/labels'.format(self.url, guid)
        res = requests.post(url, json={"labels": taglist}, headers={
                            'content-type': 'application/json'})
        if res.status_code != 200:
            raise Exception("Failed to call altas service.")
        res = res.json()['result']

        return res
