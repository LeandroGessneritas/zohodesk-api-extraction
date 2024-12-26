import pathlib
import logging
import boto3
import json
import sys
import re
import os

if "win" in sys.platform:
    from dotenv import load_dotenv
    load_dotenv()


def write_json_file(
        file_name: str,
        data: dict,
        path: str = pathlib.Path.cwd(),
        log_event: bool = True
) -> None:
    p = pathlib.Path(f"{path}")
    
    p.mkdir(exist_ok=True)

    with open(f"{p}/{file_name}.json", mode="w+", encoding="latin-1") as file:
        json.dump(data, fp=file, indent=4)

        if log_event:
            logging.info(f"File {p}/{file_name}.json saved!")


def read_json_file(path: str | pathlib.Path):
    obj = []

    with open(path, "r", encoding='latin-1') as json_file:
        obj = json.load(json_file)
    
    return obj


def __flat_json_object(obj: list | dict):
    json_file_flat = []

    for doc in obj:
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


def __flat_dict(d: dict, field_name: str) -> dict:
    d_out = {}

    for key, value in d.items():
        d_out[f"{field_name}_{key}"] = value
    
    return d_out


def normalize_json_file(
        obj: list | dict | str | pathlib.Path
) -> pathlib.Path:
    if isinstance(obj, list):
        pass
    elif isinstance(obj, dict):
        pass
    elif isinstance(obj, str):
        pass
    elif isinstance(obj, pathlib.Path):
        if obj.is_dir():
            path = list_and_sort_path(obj)

            for entry in path:
                doc = read_json_file(entry)

                flat_doc = __flat_json_object(doc)

                save_path = pathlib.Path(
                    f'{"/".join(obj.parts)}_flattened/'.replace("//", "/")
                )

                write_json_file(
                    path=save_path,
                    file_name=entry.stem,
                    data=flat_doc
                )
        elif obj.is_file():
            pass
    else:
        raise TypeError("Type not acceptable!")
    
    return pathlib.Path(save_path)


def upload_to_s3(
        filename: str,
        bucket: str,
        key: str
) -> None:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )

    s3_client.upload_file(
        Filename=filename,
        Bucket=bucket,
        Key=key
    )


def __get_int(n):
    return int(re.search(r'\d+', n.stem).group())


def list_and_sort_path(path: pathlib.Path) -> list:
    sorted_list = sorted(path.rglob("*.json"), key=__get_int)

    return sorted_list


def is_empty_folder(
        path: str | pathlib.Path
) -> bool:
    if isinstance(path, str):
        return not any(pathlib.Path(path).iterdir())
    elif isinstance(path, pathlib.Path):
        return not any(path.iterdir())
