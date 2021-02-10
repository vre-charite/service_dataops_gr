from enum import Enum

class EUserRole(Enum):
    site_admin = -1
    admin = 0
    collaborator = 1
    member = 2
    contributor = 1
    visitor = 4

def map_role_front_to_sys(role: str):
    '''
    return EUserRole Type
    '''
    return {
        'site-admin': EUserRole.site_admin,
        'admin': EUserRole.admin,
        'member': EUserRole.member,
        'contributor': EUserRole.contributor,
        'uploader': EUserRole.contributor,
        'visitor': EUserRole.visitor,
        'collaborator': EUserRole.collaborator
    }.get(role, None)

def map_role_sys_to_front(role: EUserRole):
    '''
    return string
    '''
    return {
        EUserRole.site_admin: 'site-admin',
        EUserRole.admin: 'admin',
        EUserRole.member: 'member',
        EUserRole.contributor: 'contributor',
        EUserRole.visitor: 'visitor',
        EUserRole.collaborator: 'collaborator'
    }.get(role, None)

def map_role_neo4j_to_sys(role: int):
    mapped = {
        'admin': EUserRole.admin, 
        'member': EUserRole.member,
        'uploader': EUserRole.contributor,
        'contributor': EUserRole.contributor,
        'visitor': EUserRole.visitor,
        'collaborator': EUserRole.collaborator
    }.get(role, None)
    if not role:
        raise Exception('Invalid Neo4j Role: ' + str(role))
    return mapped