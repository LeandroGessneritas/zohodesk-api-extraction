import psycopg2.extras as extras
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import pathlib
import json
import os


load_dotenv()


def get_file(file: str):
    for entry in pathlib.Path.cwd().iterdir():
        if entry.is_file() \
                and entry.suffix == ".json" \
                and file in entry.name.split('.')[0]:
            doc = []

            with open(entry, "r", encoding='latin-1') as json_file:
                doc = json.load(json_file)
    
    return doc


def flat_dict(d: dict, field_name: str) -> dict:
    d_out = {}

    for key, value in d.items():
        d_out[f"{field_name}_{key}"] = value
    
    return d_out


def normalize_file(json_file: list | dict) -> list | dict:
    json_file_flat = []

    for doc in json_file:
        aux_dict = {}

        for field in doc:
            if isinstance(doc[field], tuple):
                pass
            elif isinstance(doc[field], list):
                pass
            elif isinstance(doc[field], dict):
                aux_dict.update(flat_dict(doc[field], field))
            else:
                aux_dict[field] = doc[field]
        
        json_file_flat.append(aux_dict)
    
    return json_file_flat


def execute_values(conn, df, table: str):
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    # SQL query to execute 
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = conn.cursor() 

    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    
    print("the dataframe is inserted") 
    cursor.close()


if __name__ == "__main__":
    tickets: list = get_file(file='tickets')

    tickets_flat = normalize_file(tickets)

    df_tickets = pd.DataFrame.from_dict(tickets_flat)

    conn = psycopg2.connect( 
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
    )

    execute_values(conn, df_tickets, 'raw_zohodesk.tickets') 
