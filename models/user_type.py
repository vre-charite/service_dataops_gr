from enum import Enum

class EUserRole(Enum):
    site_admin = -1
    admin = 0
    member = 1
    contributor = 2
    visitor = 3

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
        'visitor': EUserRole.visitor
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
        EUserRole.visitor: 'visitor'
    }.get(role, None)

def map_role_neo4j_to_sys(role: int):
    return {
        'admin': EUserRole.admin, 
        'member': EUserRole.member,
        'uploader': EUserRole.contributor,
        'contributor': EUserRole.contributor,
        'visitor': EUserRole.visitor
    }.get(role, None)