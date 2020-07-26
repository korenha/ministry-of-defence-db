from database_exercise.db_api import DataBase
from MyDBField import MyDBField
from MyDBTable import MyDBTable
from MySelectionCriteria import MySelectionCriteria
from typing import Any, Dict, List, Type
import os


class MyDataBase(DataBase):
    __data_base_id = 1

    def __init__(self):
        self.__data_base_path = "DataBases/MyDatabase" + str(MyDataBase.__data_base_id)
        self.__num_of_tables = 0
        os.mkdir(self.__data_base_path)
        MyDataBase.__data_base_id += 1

    def create_table(self,
                     table_name: str,
                     fields: List[MyDBField],
                     key_field_name: str) -> MyDBTable:
        raise NotImplementedError

    def num_tables(self) -> int:
        raise NotImplementedError

    def get_table(self, table_name: str) -> MyDBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[MySelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
