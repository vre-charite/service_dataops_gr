class MetaAPI(type):
    def __new__(cls, name: str, bases, namespace, **kwargs):
        if not name.startswith('API'):
            raise TypeError('[Fatal] Invalid API Statement: class name should start with "API"', name)
        if 'api_registry' not in namespace:
            raise TypeError('[Fatal] Invalid API Statement: Function api_registry Not Found ', name)
        return super().__new__(cls, name, bases, namespace, **kwargs)