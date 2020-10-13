from flask import request

def get_session_id():
    session_id = request.headers.get('Session-ID')
    return session_id