from typing import List
from urllib.parse import quote_plus
import getpass
import json
import os
import pandas as pd
import pathlib
import shutil
import time


class DataRepoError(Exception):

    def __init__(self, message, key=None, library=None, filename=None):
        self._key = key
        self._library = library

        # Call the base class constructor with the parameters it needs
        extras = []
        if filename:
            extras.append(f"filename:{filename}")
        if library:
            extras.append(f"library:{library}")
        if key:
            extras.append(f"key:{key}")
        if extras:
            message += ", " + ", ".join(extras)
        super().__init__(message)

    @property
    def key(self):
        return self._key

    @property
    def library(self):
        return self._library

class DataRepo:

    def __init__(self,
                 storage_path):
        self._path = pathlib.Path(storage_path)
        os.makedirs(self._path, exist_ok=True)

    def __str__(self):
        return f"DataRepo('{self._path}')"

    @staticmethod
    def _validate_names(library, key):
        if key is not None:
            assert isinstance(key, str)
            if key == "":
                raise DataRepoError("key cannot be empty")
        if library is not None:
            assert isinstance(library, str)
            if library == "":
                raise DataRepoError("library name cannot be empty", library=library)
            if library.startswith("."):
                raise DataRepoError("library name cannot start with period", library=library)

    def list_libraries(self) -> List[str]:
        return [f.name for f in os.scandir(self._path) if f.is_dir()]

    def create_library(self, name, exist_ok=True):
        lib_url = self._path / name
        os.makedirs(lib_url, exist_ok=exist_ok)

    def delete_library(self, name):
        path = self._path / name
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass

    def has_library(self, name: str) -> bool:
        lib_url = self._path / name
        return os.path.isdir(lib_url)

    def get_library(self, name, auto_create: bool = True):
        if not self.has_library(name) and auto_create:
            self.create_library(name)
        if not self.has_library(name):
            raise DataRepoError("library not found", library=name)
        return Library(self, name)

    def _build_path(self, library: str, key: str, suffix):
        quoted_name = quote_plus(key)
        path = self._path / library / quoted_name
        path = path.with_suffix(path.suffix + suffix)
        return path

    def _build_meta_path(self, library: str, key: str):
        return self._build_path(library, key, ".meta.json")

    def _load_item_meta(self, library, key):
        filename = self._build_meta_path(library, key)
        try:
            with open(filename) as f:
                return json.load(f)
        except FileNotFoundError:
            raise DataRepoError("item not found", key=key, library=library)

    @staticmethod
    def _is_meta_file(path):
        return len(path.suffixes) > 1 \
            and path.suffixes[-1] == ".json" \
            and path.suffixes[-2] == ".meta"

    def _list_keys(self, library: str):
        self._validate_names(library, key=None)
        path = self._path / library
        all_files = [pathlib.Path(f) for f in os.scandir(path) if f.is_file()]
        meta_files = [x for x in all_files if self._is_meta_file(x)]

        keys = []
        for json_file in meta_files:
            with open(json_file, "r") as f:
                meta = json.load(f)
                keys.append(meta["key"])
        return keys

    def _read_item(self, library: str, key: str):
        self._validate_names(library, key)
        full_path = self._path / library
        meta_data = self._load_item_meta(library, key)
        data_filename = full_path / meta_data["filename"]
        if meta_data["type"] == "dataframe":
            return pd.read_parquet(data_filename)
        return None

    def _delete_item(self, library: str, key: str):
        self._validate_names(library, key)
        full_path = self._path / library
        meta_path = self._build_meta_path(library, key)
        meta_data = self._load_item_meta(library, key)
        data_filename = full_path / meta_data["filename"]
        os.unlink(meta_path)
        os.unlink(data_filename)

    def _write_item(self, library: str, key: str, data):
        self._validate_names(library, key)
        meta_filename = self._build_meta_path(library, key)
        full_path = self._path / library

        # create the new meta
        if isinstance(data, pd.DataFrame):
            data_filename = self._build_path(library, key, ".data.parq")
            data_filename_tmp = self._build_path(library, key, ".temp.parq")
            meta = {
                "type": "dataframe",
                "update_time": time.time(),  # noqa
                "filename": data_filename.name,
                "user": getpass.getuser(),
                "key": key
            }
        else:
            raise DataRepoError("data type not supported")

        # write the new item to a temporary location
        meta_filename_tmp = self._build_path(library, key, ".temp.json")

        # create the library directory, if not exists
        os.makedirs(full_path, exist_ok=True)

        with open(meta_filename_tmp, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)

        data.to_parquet(data_filename_tmp)

        if os.path.isfile(meta_filename):
            existing_meta = self._load_item_meta(library, key)
            existing_filename = full_path / existing_meta["filename"]
            os.unlink(meta_filename)
            os.unlink(existing_filename)

        # move the temp items to final names
        os.rename(data_filename_tmp, data_filename)
        os.rename(meta_filename_tmp, meta_filename)


class Library:

    def __init__(self, repo: DataRepo, name: str):
        self._repo = repo
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def repo(self):
        return self._repo

    def list_keys(self):
        return self._repo._list_keys(self._name)  # noqa

    def read(self, key):
        return self._repo._read_item(self._name, key)  # noqa

    def write(self, key, data):
        return self._repo._write_item(self._name, key, data)  # noqa

    def delete(self, key: str):
        return self._repo._delete_item(self._name, key)  # noqa
