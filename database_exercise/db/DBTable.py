import db_api
from .DBField import DBField
from .SelectionCriteria import SelectionCriteria
from typing import Any, Dict, List
import os
from bson import BSON
import bson
from operator import eq, ne, gt, lt, le, ge, is_, is_not
import pickle
from pathlib import Path
from collections import defaultdict


operator_dict = {"<": lt, ">": gt, "=": eq, "!=": ne, "<=": le, ">=": ge, "is": is_, "is not": is_not}


class DBTable(db_api.DBTable):

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        self.__NAME = name
        self.__FIELDS = fields
        self.__KEY_FIELD_NAME = key_field_name
        self.__PATH = os.path.join(db_api.DB_ROOT, name)
        self.__METADATA_PATH = os.path.join(self.__PATH, "metadata.db")
        self.__TABLE_NAME__PATH = os.path.join(self.__PATH, name)
        self.__MAX_ROWS = 1000
        self.__num_rows = 0
        self.__num_of_blocks = 1
        self.__blocks_have_place = {1: self.__MAX_ROWS}
        self.__indexes = {self.__KEY_FIELD_NAME:f"{self.__TABLE_NAME__PATH}_{self.__KEY_FIELD_NAME}_index.db"}
        os.mkdir(Path(self.__PATH))
        with open(self.__indexes[self.__KEY_FIELD_NAME], "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))

        with open(self.__TABLE_NAME__PATH + "1.db", "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))

        with open(self.__METADATA_PATH, "wb"):
            pass

    def __backup(self):
        info = {"num_rows": self.__num_rows,
                "num_of_blocks": self.__num_of_blocks,
                # "path":self.__my_path,
                "blocks_have_place": self.__blocks_have_place,
                "indexes": self.__indexes}
        pickle.dump(info, open(self.__METADATA_PATH, "wb"))

    def reload_backup(self):
        try:
            data = pickle.load(open(self.__METADATA_PATH, "rb"))
            self.__num_rows = data["num_rows"]
            self.__num_of_blocks = data["num_of_blocks"]
            self.__blocks_have_place = data["blocks_have_place"]
            self.__indexes = data["indexes"]
        except FileNotFoundError:
            pass

    def count(self) -> int:
        return self.__num_rows

    def insert_record(self, values: Dict[str, Any]) -> None:
        # להוסיף טיפול ב-self.__blocks_have_place
        path = self.__TABLE_NAME__PATH + "1.db"
        if self.__get_path_of_key(str(values[self.__KEY_FIELD_NAME])) is not None:
            raise ValueError
        try:
            for field in self.__FIELDS:
                if type(values[field.name]) != field.type:
                    pass
        except KeyError:
            raise ValueError
        with open(path, "rb") as bson_file:
            dict_ = bson.decode_all(bson_file.read())[0]
            dict_[str(values[self.__KEY_FIELD_NAME])] = values
            print(dict_)
        with open(path, "wb") as bson_file:
            bson_file.write(BSON.encode(dict_))

        self.__num_rows += 1
        for field_name in self.__indexes:
            with open(self.__indexes[field_name], "rb") as bson_file:
                index_dict = defaultdict(dict)
                index_dict.update(bson.decode_all(bson_file.read())[0])
                index_dict[str(values[field_name])][str(values[self.__KEY_FIELD_NAME])] = path
            with open(self.__indexes[field_name], "wb") as bson_file:
                bson_file.write(BSON.encode(index_dict))
        self.__backup()

    def delete_record(self, key: Any) -> None:
        path = self.__get_path_of_key(str(key))
        if path is None:
            raise ValueError
        with open(path, "rb") as bson_file:
            dict_ = bson.decode_all(bson_file.read())[0]
            del dict_[str(key)]
        with open(path, "wb") as bson_file:
            bson_file.write(BSON.encode(dict_))

        for field_name in self.__indexes:
            with open(self.__indexes[field_name], "rb") as bson_file:
                index_dict = bson.decode_all(bson_file.read())[0]
                del index_dict[str(key)]
            with open(self.__indexes[field_name], "wb") as bson_file:
                bson_file.write(BSON.encode(index_dict))
        self.__num_rows -= 1
        self.__backup()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        # צריך לממש אופטימיזציות עם אינדקס
        keys_to_delete = []
        for i in range(self.__num_of_blocks):
            with open(self.__TABLE_NAME__PATH + f"{i + 1}.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meeting_conditions(dict_[key], criteria):
                        keys_to_delete.append(key)
                for key in keys_to_delete:
                    del dict_[str(key)]

            with open(self.__TABLE_NAME__PATH + f"{i + 1}.db", "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

        with open(self.__indexes[self.__KEY_FIELD_NAME], "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())[0]
            for key in keys_to_delete:
                del keys_dict[key]
                self.__num_rows -= 1
        with open(self.__indexes[self.__KEY_FIELD_NAME], "wb") as bson_file:
            bson_file.write(BSON.encode(keys_dict))

        self.__backup()

    def get_record(self, key: Any) -> Dict[str, Any]:
        path = self.__get_path_of_key(str(key))
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                return dict_[str(key)]
        return {}

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        path = self.__get_path_of_key(str(key))
        if path is None:
            raise ValueError
        with open(path, "rb") as bson_file:
            dict_ = bson.decode_all(bson_file.read())[0]
            dict_[str(key)].update(values)
        with open(path, "wb") as bson_file:
            bson_file.write(BSON.encode(dict_))

        self.__backup()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        # צריך לעשות אופטימיזציות
        query_list = []
        for i in range(self.__num_of_blocks):
            with open(self.__TABLE_NAME__PATH + f"{i + 1}.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meeting_conditions(dict_[str(key)], criteria):
                        query_list.append(dict_[str(key)])

        return query_list

    def create_index(self, field_to_index: str) -> None:
        # check if it not exist
        self.__indexes[field_to_index] = f"{self.__TABLE_NAME__PATH}_{field_to_index}_index.db"
        index_dict = defaultdict(dict)
        for index in range(self.__num_of_blocks):
            with open(self.__TABLE_NAME__PATH + f"{index+1}.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    index_dict[dict_[key][field_to_index]][key] = self.__get_path_of_key(key)
                    # check th overflow block
        with open(self.__indexes[field_to_index], "wb") as index_bson_file:
            index_bson_file.write(BSON.encode(index_dict))

    def __get_path_of_key(self, key: Any) -> str:
        with open(self.__indexes[self.__KEY_FIELD_NAME], "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())
            try:
                return keys_dict[0][key][key]
            except KeyError:
                return None

    @staticmethod
    def __is_meeting_conditions(item, criteria: List[SelectionCriteria]) -> bool:
        for select in criteria:
            first = item[select.field_name]
            operator = select.operator
            value = select.value
            if not operator_dict[operator](first, value):
                return False
        return True