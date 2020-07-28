import db_api
from .DBField import DBField
from .SelectionCriteria import SelectionCriteria
from typing import Any, Dict, List
import os
from bson import BSON
import bson
from operator import eq, ne, gt, lt, le, ge, is_, is_not
import pickle

operator_dict = {"<": lt, ">": gt, "=": eq, "!=": ne, "<=": le, ">=": ge, "is": is_, "is not": is_not}


class DBTable(db_api.DBTable):
    __max_rows = 1000

    def __init__(self, name: str, fields: List[DBField], key_field_name: str, data_base_path=""):
        self.__NAME = name
        self.__FIELDS = fields
        self.__KEY_FIELD_NAME = key_field_name
        self.__INFO_PATH = os.path.join(data_base_path, "table_info.db")
        self.__MY_PATH = os.path.join(data_base_path, name)
        self.__PATH = data_base_path
        self.__num_rows = 0
        self.__num_of_blocks = 1
        self.__blocks_have_place = {1: 1000}
        self.__indexes = {self.__KEY_FIELD_NAME}
        with open(self.__MY_PATH + "_key_index.db", "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))

        with open(self.__MY_PATH + "1.db", "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))
        try:
            with open(self.__INFO_PATH, "rb"):
                pass
        except FileNotFoundError:
            with open(self.__INFO_PATH, "wb"):
                pass

    def __backup(self):
        info = {"num_rows": self.__num_rows,
                "num_of_blocks": self.__num_of_blocks,
                # "path":self.__my_path,
                "blocks_have_place": self.__blocks_have_place,
                "indexes": self.__indexes}
        pickle.dump(info, open(self.__INFO_PATH, "wb"))

    def reload_backup(self):
        try:
            data = pickle.load(open(self.__INFO_PATH, "rb"))
            self.__num_rows = data["num_rows"]
            self.__num_of_blocks = data["num_of_blocks"]
            self.__blocks_have_place = data["blocks_have_place"]
            self.__indexes = data["indexes"]
        except:
            pass

    def count(self) -> int:
        return self.__num_rows

    def insert_record(self, values: Dict[str, Any]) -> None:
        # להוסיף טיפול ב-self.__blocks_have_place
        if self.__get_path_of_key(str(values[self.__KEY_FIELD_NAME])) is not None:
            raise ValueError
        else:
            try:
                for field in self.__FIELDS:
                    if type(values[field.name]) != field.type:
                        pass
            except KeyError:
                raise ValueError
            with open(self.__MY_PATH + "1.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                dict_[str(values[self.__KEY_FIELD_NAME])] = values
                print(dict_)
            with open(self.__MY_PATH + "1.db", "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

            self.__num_rows += 1
            with open(self.__MY_PATH + "_key_index.db", "rb") as bson_file:
                keys_dict = bson.decode_all(bson_file.read())[0]
                keys_dict[str(values[self.__KEY_FIELD_NAME])] = self.__MY_PATH + "1.db"
            with open(self.__MY_PATH + "_key_index.db", "wb") as bson_file:
                bson_file.write(BSON.encode(keys_dict))

            self.__backup()

    def delete_record(self, key: Any) -> None:
        path = self.__get_path_of_key(str(key))
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                del dict_[str(key)]
            with open(path, "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

            with open(self.__MY_PATH + "_key_index.db", "rb") as bson_file:
                keys_dict = bson.decode_all(bson_file.read())[0]
                del keys_dict[str(key)]
            with open(self.__MY_PATH + "_key_index.db", "wb") as bson_file:
                bson_file.write(BSON.encode(keys_dict))
            self.__num_rows -= 1
            self.__backup()
        else:
            raise ValueError

    def __is_meets_conditions(self, item, criteria: List[SelectionCriteria]) -> bool:
        for select in criteria:
            first = item[select.field_name]
            operator = select.operator
            value = select.value
            if not operator_dict[operator](first, value):
                return False
        return True

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        # צריך לממש אופטימיזציות עם אינדקס
        keys_to_delete = []
        for i in range(self.__num_of_blocks):
            with open(self.__MY_PATH + f"{i + 1}.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meets_conditions(dict_[key], criteria):
                        keys_to_delete.append(key)
                for key in keys_to_delete:
                    del dict_[str(key)]

            with open(self.__MY_PATH + f"{i + 1}.db", "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

        with open(self.__MY_PATH + "_key_index.db", "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())[0]
            for key in keys_to_delete:
                del keys_dict[key]
                self.__num_rows -= 1
        with open(self.__MY_PATH + "_key_index.db", "wb") as bson_file:
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
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                dict_[str(key)].update(values)
            with open(path, "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))
        else:
            print("the key is not exist")
        self.__backup()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        # צריך לעשות אופטימיזציות
        query_list = []
        for i in range(self.__num_of_blocks):
            with open(self.__MY_PATH + f"{i + 1}.db", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meets_conditions(dict_[str(key)], criteria):
                        query_list.append(dict_[str(key)])

        return query_list

    def create_index(self, field_to_index: str) -> None:
        pass

    def __get_path_of_key(self, key: Any) -> str:
        with open(self.__MY_PATH + "_key_index.db", "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())
            try:
                return keys_dict[0][key]
            except KeyError:
                return None
