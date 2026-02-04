import os
from typing import List


class FileManager():
    def __init__(self, dir_path: str):
        self.dir_path = dir_path

    def discover_files(self) -> List[str]:
        """Create a list of all the files in a dir that need parsing"""
        file_list: List[str] = []
        for root, dirs, files in os.walk(self.dir_path):
            for file in files:
                if file.endswith(".sql") or file.endswith(".py") or file.endswith(".ipynb"):
                    file_list.append(os.path.join(root, file))

        return file_list

    def read_file(self, file: str) -> str:
        with open(file, "r") as f:
            file_str = f.read()

        return file_str
