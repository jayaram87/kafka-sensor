import pandas as pd
import json

class Generic:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def dict_to_object(data: dict, ctx):
        return Generic(data)

    @classmethod
    def get_object(cls, path):
        df = pd.read_csv(path, chunksize=10)
        row = 0
        for chunk in df:
            for data in chunk.values:
                dict_data = Generic(dict(zip(chunk.columns, list(map(str, data)))))
                row += 1
                yield dict_data

    @classmethod
    def get_topic_schema(cls, path):
        cols = next(pd.read_csv(path, chunksize=5)).columns

        schema = dict()

        schema.update({
            "$id": "http://example.com/myURI.schema.json",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "additionalProperties": False,
            "description": "Sample schema to help you get started.",
            "properties": dict(),
            "title": "SampleRecord",
            "type": "object"
        })

        for col in cols:
            schema['properties'].update({
                f"{col}": {"description": f"{col} field", "type": "string"}
            })

        schema = json.dumps(schema)

        return schema

    def __str__(self):
        return f'{self.__dict__}'

def instance_to_dict(instance: Generic, idx):
    return instance.to_dict()
