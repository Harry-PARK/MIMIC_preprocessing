from sqlalchemy import create_engine

class Config():
    def __init__(self):
        pass

class DBConfig():
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, value):
        self._engine = value
