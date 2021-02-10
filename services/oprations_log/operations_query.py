from datetime import datetime, timedelta
import requests
from config import ConfigClass

def query(page, page_size, operation_type, project_code, start_date, end_date, owner, operator, role):
    '''
        fetch operations logs
    '''
    try:
        if start_date == None or end_date == None:
            today_date = datetime.now().date()
            today_datetime = datetime.combine(today_date, datetime.min.time())
            start_date = int(today_datetime.timestamp())

            end_datetime = datetime.combine(
                today_date + timedelta(days=1), datetime.min.time())
            end_date = int(end_datetime.timestamp())
        else:
            start_date = int(start_date)
            end_date = int(end_date)

        type_name = 'file_operation_logs'

        criterion = [
            {
                'attributeName': 'bucketName',
                'attributeValue': project_code,
                'operator': 'eq'
            },
            {
                'attributeName': 'createTime',
                'attributeValue': int(start_date),
                'operator': 'gte'
            },
            {
                'attributeName': 'createTime',
                'attributeValue': int(end_date),
                'operator': 'lte'
            },
            {
                'attributeName': 'operationType',
                'attributeValue': operation_type,
                'operator': 'eq'
            }
        ]

        # if operation_type == 'data_delete':
        #     criterion.append({
        #         'attributeName': 'processed_pipeline',
        #         'attributeValue': operation_type,
        #         'operator': 'eq'
        #     })
        #     type_name = 'file_data'
        # else:
            # criterion.append({
            #     'attributeName': 'operationType',
            #     'attributeValue': operation_type,
            #     'operator': 'eq'
            # })
        

        
        if owner:
            criterion.append({
                'attributeName': 'owner',
                'attributeValue': owner,
                'operator': 'eq'
            })
        if operator:
            criterion.append({
                'attributeName': 'operator',
                'attributeValue': operator,
                'operator': 'eq'
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
            'attributes': ['owner', 'operator', 'fileName', 'file_name', 'createTime', 'originPath', 'path', 'operationType'],
            'sortBy': 'createTime',
            'sortOrder': 'DESCENDING',
            'typeName': type_name,
            'classification': None,
            'termName': None
        }

        if not page:
            page = 0

        if page_size:
            post_data['limit'] = page_size
            post_data['offset'] = str(int(page) * int(page_size))
        

        res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                            json=post_data, headers={'content-type': 'application/json'})
        if res.status_code != 200:
            return {'result': res.json()}, 403
        res = res.json()['result']

        if not res.get('entities', None):
            res.update({'entities': []})

        return res

    except Exception as e:
        print(srt(e))
