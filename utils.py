# import psycopg2.extras as extras
from typing import Literal
# import pandas as pd
# import psycopg2
import pathlib
import logging
import boto3
import json
import sys
# import os

if "win" in sys.platform:
    from dotenv import load_dotenv
    load_dotenv()


def write_json_file(
        path: str,
        file_name: str,
        data: dict,
) -> None:
    p = pathlib.Path(f"{path}")
    
    p.mkdir(exist_ok=True)

    with open(f"{p}/{file_name}.json", mode="w+", encoding="utf-8") as file:
        json.dump(data, fp=file, indent=4, ensure_ascii=True)

        logging.info(f"File {p}/{file_name}.json saved!")


# def get_file(
#         file: str,
# ) -> None | list | dict:
#     if level == 'folder':
#         for entry in pathlib.Path(path).iterdir():
#             if entry.is_file() \
#                     and entry.suffix == ".json" \
#                     and file in entry.name.split('.')[0]:
#                 doc = []

#                 with open(entry, "r", encoding='latin-1') as json_file:
#                     doc = json.load(json_file)
    
#     return doc


def __flat_dict(d: dict, field_name: str) -> dict:
    d_out = {}

    for key, value in d.items():
        d_out[f"{field_name}_{key}"] = value
    
    return d_out


def normalize_file(
        file: list | dict,
        level: Literal['folder', 'file'] = 'folder',
        path: str = './',
) -> list | dict:
    if level == 'folder':
        pass
    else:
        pass

    json_file_flat = []

    for doc in file:
        aux_dict = {}

        for field in doc:
            if isinstance(doc[field], tuple):
                pass
            elif isinstance(doc[field], list):
                pass
            elif isinstance(doc[field], dict):
                aux_dict.update(__flat_dict(doc[field], field))
            else:
                aux_dict[field] = doc[field]
        
        json_file_flat.append(aux_dict)
    
    return json_file_flat

def upload_to_s3(
        bucket: str,
        key: str,
        object: str | None = None
) -> None:
    s3_client: boto3.client = boto3.client("s3")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=object
    )


# def execute_values(conn, df, table: str):
#     tuples = [tuple(x) for x in df.to_numpy()] 
  
#     cols = ','.join(list(df.columns)) 
#     # SQL query to execute 
#     query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
#     cursor = conn.cursor() 

#     try: 
#         extras.execute_values(cursor, query, tuples) 
#         conn.commit() 
#     except (Exception, psycopg2.DatabaseError) as error: 
#         print("Error: %s" % error) 
#         conn.rollback() 
#         cursor.close() 
#         return 1
    
#     print("the dataframe is inserted") 
#     cursor.close()


# if __name__ == "__main__":
    # conn = psycopg2.connect( 
    #     database=os.getenv("DATABASE"),
    #     user=os.getenv("USER"),
    #     password=os.getenv("PASSWORD"),
    #     host=os.getenv("HOST"),
    #     port=os.getenv("PORT"),
    # )
