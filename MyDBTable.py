from database_exercise.db_api import DBTable
from MyDBField import MyDBField
from MySelectionCriteria import MySelectionCriteria
from typing import Any, Dict, List


class MyDBTable(DBTable):
    def __init__(self, name: str, fields: List[MyDBField], key_field_name: str):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[MySelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[MySelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError
