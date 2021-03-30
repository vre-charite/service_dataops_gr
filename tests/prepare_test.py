from app import create_app


class SetUpTest:

    def __init__(self, log):
        self.log = log
        self.app = PrepareTest().app

    def create_item(self):
        pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class PrepareTest(metaclass=Singleton):

    def __init__(self):
        self.app = self.create_test_client()

    def create_test_client(self):
        app = create_app()
        app.config['TESTING'] = True
        app.config['DEBUG'] = True
        test_client = app.test_client(self)
        return test_client
