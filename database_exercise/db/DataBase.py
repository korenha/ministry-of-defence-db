import db_api
from .DBField import DBField
from .DBTable import DBTable
from .SelectionCriteria import SelectionCriteria
from typing import Any, Dict, List
import os
import shutil
import pickle
from distutils.dir_util import copy_tree
from bplustree import BPlusTree
from pathlib import Path

# צריך לגבות את כל האוביקטים ןליצור אפשרות להשתמש בבסיס נתונים קיים


class DataBase(db_api.DataBase):
    __TABLES = dict()
    __METADATA_PATH = Path(f"{db_api.DB_ROOT}/METADATA.db")

    def __init__(self):
        if not Path(f"{db_api.DB_ROOT}/DB.db").is_file():
            pickle.dump(DataBase.__TABLES, open(os.path.join(db_api.DB_ROOT,"DB.db"), "wb"))

        else:
            DataBase.__TABLES = pickle.load(open(os.path.join(db_api.DB_ROOT, "DB.db"), "rb"))
            for table in DataBase.__TABLES.values():
                table.reload_backup()

    def __add_table_info(self, info, name):
        DataBase.__TABLES[name] = info
        pickle.dump(DataBase.__TABLES, open(os.path.join(db_api.DB_ROOT, "DB.db"), "wb"))

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        try:
            new_table = DBTable(table_name, fields, key_field_name)
            DataBase.__TABLES[table_name] = new_table
            self.__add_table_info(new_table, table_name)
            return new_table
        except FileExistsError:
            print(f"{table_name} is already exist")
            return DataBase.__TABLES[table_name]

    def num_tables(self) -> int:
        return len(DataBase.__TABLES)

    def get_table(self, table_name: str) -> DBTable:
        return DataBase.__TABLES[table_name]

    def delete_table(self, table_name: str) -> None:
        try:
            shutil.rmtree(os.path.join(db_api.DB_ROOT, table_name))
            del DataBase.__TABLES[table_name]
        except FileNotFoundError:
            print(f"Failed to delete table {table_name}")

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__TABLES.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        pass
