import pymongo
import certifi
from src.constants.secrets import *


class DBOps:
    def __init__(self) -> None:
        self.client = pymongo.MongoClient(mongodb_url, tlsCAFile=certifi.where())
        self.db = 'sample'

    def insert_many(self, col_name, records: list):
        self.client[self.db][col_name].insert_many(records)

    def insert_one(self, col_name, record):
        self.client[self.db][col_name].insert_one(record)