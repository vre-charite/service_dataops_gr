from models.user_type import EUserRole, map_role_front_to_sys

def get_security_lvl(user_role: EUserRole):
    '''
    return security level in int type, possible to be changed in the future
    '''
    max_lvl = 999999
    return max_lvl - user_role.value

def user_accessible(required_role: EUserRole, user_role: EUserRole):
    '''
    check user security level and access.
    return True Or False
    '''
    return get_security_lvl(user_role) >= get_security_lvl(required_role)