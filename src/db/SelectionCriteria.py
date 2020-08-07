import db_api
from typing import Any


class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value
