from database_exercise.db_api import DBField
from typing import Type


class MyDBField(DBField):
    def __init__(self, name: str, type_: Type):
        self.name = name
        self.type = type_
