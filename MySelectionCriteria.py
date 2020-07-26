from database_exercise.db_api import SelectionCriteria
from typing import Any


class MySelectionCriteria(SelectionCriteria):
    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value
