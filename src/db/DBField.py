import db_api
from typing import Type


class DBField(db_api.DBField):
    def __init__(self, name: str, type_: Type):
        self.name = name
        self.type = type_