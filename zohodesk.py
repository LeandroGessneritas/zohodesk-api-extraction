from utils import (
    write_json_file, 
    read_json_file, 
    send_data_to_s3,
    get_infos,
    update_infos
)
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Literal
import requests as req
import logging
import pathlib
import json
import sys
import os
import re

import pandas as pd
import s3fs


logging.basicConfig(level=logging.INFO)


@dataclass
class Organizations:
    companyName: str
    companyId: str


class Zohodesk:
    def __init__(
            self,
            code: Optional[str] = None,
    ) -> None:
        self.base_url: str = "https://desk.zoho.com/api/v1"
        self.token_url: str = "https://accounts.zoho.com/oauth/v2/token"
        self.__client_id: str = os.getenv("CLIENT_ID")
        self.__client_secret: str = os.getenv("CLIENT_SECRET")
        self.code: str = code
        # TODO: increase pattern for date "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        self.__date_pattern = r"2[0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z"
        self.__org_id = self.get_organizations().companyId

        if "win" in sys.platform:
            from dotenv import load_dotenv
            load_dotenv()

    def __generate_refresh_token(self) -> None:
        logging.warning("Generating refresh token...")

        if self.code is None or self.code == "":
            raise Exception("Code is needed to get refresh token")
        
        # it must get the code parameter in api console before running
        response = req.post(
            url=self.token_url,
            params={
                "code": self.code,
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "grant_type": "authorization_code"
            }
        )

        json_response: dict = json.loads(response.content)

        if "error" in f"{json_response}":
            raise Exception("The provided code is invalid. Generate a new one in the API Console Portal.")

        update_infos(key="refresh_token", value=json_response.get("refresh_token"))

        logging.warning("DONE.")

    def __get_refresh_token(self) -> str:
        if self.code is not None:
            last_code = get_infos(key="last_api_code")
            
            if last_code != self.code:
                update_infos(
                    key="last_api_code",
                    value=self.code
                )
                
                self.__generate_refresh_token()

        if "refresh_token" not in list(get_infos().keys()):
            self.__generate_refresh_token()
        
        return get_infos("refresh_token")
    
    def __get_token(self) -> str:
        refresh_token: str = self.__get_refresh_token()

        resp = req.post(
            url=self.token_url,
            params={
                "refresh_token": refresh_token,
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "grant_type": "refresh_token"
            }
        )

        content: dict = json.loads(resp.content)

        try:
            # try to return the token that is valid for more than one hour
            return content['access_token']
        except KeyError:
            # print the error message and terminate the script running
            error_list_keys = list(content.keys())
            error_message = content.get('error_description') if "error_description" in error_list_keys else content.get('error')
            logging.info(error_message)
            sys.exit()
    
    def get_organizations(self) -> Organizations:
        token = self.__get_token()

        response = req.get(
            url=f"{self.base_url}/organizations",
            headers={
                "Authorization": f"Zoho-oauthtoken {token}"
            }
        )

        if response.status_code == 200:
            data = json.loads(response.content)['data'][0]

            return Organizations(
                companyName=data['companyName'],
                companyId=str(data['id'])
            )
    
    def get_tickets(
            self,
            orgId: Optional[str] = None,
            save_path: Optional[str] = './tickets',
            start_date: str = "",
            upload: bool = True,
    ) -> None | pathlib.Path:
        # getting the token
        token = self.__get_token()

        if orgId is None:
            orgId = self.__org_id 
        
        if start_date != "":
            # validating the date passed as parameter
            valid_date = re.match(self.__date_pattern, start_date)

            if bool(valid_date) is False:
                raise ValueError(
                    """Valid date is required to get only updated tickets.
                    Expected format is yyyy-MM-dd'T'HH:mm:ss.SSS'Z' wihtout the quotes."""
                )
        else:
            # veryfing if already exist downloaded tickets
            try:
                file: dict = read_json_file(path=pathlib.Path("last_ticket.json").absolute())

                start_date = file.get("last_ticket_downloaded_date")
            except FileNotFoundError:
                start_date = "2018-01-01T00:00:00.000Z"
        
        today = datetime.today()
        full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

        endpoint: str = "tickets"
        parameter: str = f"search?modifiedTimeRange={start_date},{full_last_hour_today}"
        sort_by: str = "modifiedTime"
        start: int = 0

        for num in range(start, start + 5_000, 100):
            response = req.get(
                url=f"{self.base_url}/{endpoint}/{parameter}&from={num}&limit=100&sortBy={sort_by}",
                headers={
                    "orgId": orgId,
                    "Authorization": f"Zoho-oauthtoken {token}"
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                init: str = data[0]["modifiedTime"]
                final: str = data[-1]["modifiedTime"]
                
                init_replaces: str = init.replace(':', '-').replace('T', '_').replace('.000Z', '')
                final_replaces: str = final.replace(':', '-').replace('T', '_').replace('.000Z', '')

                write_json_file(
                    path=save_path,
                    file_name=f"tickets_from_{init_replaces}_to_{final_replaces}",
                    data=data
                )

                write_json_file(
                    file_name="last_ticket",
                    data={"last_ticket_downloaded_date": f"{final}"},
                    log_event=False
                )
            elif response.status_code == 204:
                break
            else:
                pass
        
        saved_tickets_path = pathlib.Path("./tickets").absolute()

        if upload:
            send_data_to_s3(
                saved_tickets_path,
                bucket="501464632998-prod-landing-corporate",
                key="zohodesk/tickets"
            )

            if response.status_code == 204:
                sys.exit()
        else:
            return pathlib.Path("./tickets").absolute()
    
    def get_departments(self) -> None:
        token = self.__get_token()

        response = req.get(
            url=f"{self.base_url}/departments",
            headers={
                "Authorization": f"Zoho-oauthtoken {token}"
            }
        )

        if response.status_code == 200:
            data = json.loads(response.content)['data']

            write_json_file("departamentos", data=data, path="./")

    def get_products(self) -> None:
        token = self.__get_token()

        response = req.get(
            url=f"{self.base_url}/products",
            headers={
                "Authorization": f"Zoho-oauthtoken {token}"
            }
        )

        if response.status_code == 200:
            data = json.loads(response.content)['data']

            write_json_file("produtos", data=data, path="./")

    def get_tasks(
            self,
            orgId: Optional[str] = None,
            save_path: Optional[str] = './tasks',
            start_date: str = "",
            upload: bool = True,
    ) -> None:
        token = self.__get_token()

        if orgId is None:
            orgId = self.__org_id 

        if start_date != "":
            # validating the date passed as parameter
            valid_date = re.match(self.__date_pattern, start_date)

            if bool(valid_date) is False:
                raise ValueError(
                    """Valid date is required to get only updated tickets.
                    Expected format is yyyy-MM-dd'T'HH:mm:ss.SSS'Z' wihtout the quotes."""
                )
        else:
            # veryfing if already exist downloaded tickets
            try:
                file: dict = read_json_file(path=pathlib.Path("last_task.json").absolute())

                start_date = file.get("last_task_downloaded_date")
            except FileNotFoundError:
                start_date = "2018-01-01T00:00:00.000Z"
        
        today = datetime.today()
        full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

        parameter: str = f"search?modifiedTimeRange={start_date},{full_last_hour_today}"
        sort_by: str = "modifiedTime"
        endpoint: str = "tasks"
        start: int = 0

        for num in range(start, start + 5_000, 100):
            response = req.get(
                url=f"{self.base_url}/{endpoint}/{parameter}&from={num}&limit=100&sortBy={sort_by}",
                headers={
                    "Authorization": f"Zoho-oauthtoken {token}",
                    "orgId": orgId
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                init: str = data[0]["modifiedTime"]
                final: str = data[-1]["modifiedTime"]
                
                init_replaces: str = init.replace(':', '-').replace('T', '_').replace('.000Z', '')
                final_replaces: str = final.replace(':', '-').replace('T', '_').replace('.000Z', '')

                write_json_file(
                    path=save_path,
                    file_name=f"tasks_from_{init_replaces}_to_{final_replaces}",
                    data=data
                )

                write_json_file(
                    file_name="last_task",
                    data={"last_task_downloaded_date": f"{final}"},
                    log_event=False
                )
            elif response.status_code == 204:
                break
            else:
                pass
        
        saved_tasks_path = pathlib.Path("./tasks").absolute()

        if upload:
            send_data_to_s3(
                saved_tasks_path,
                bucket="501464632998-prod-landing-corporate",
                key="zohodesk/tasks"
            )

            if response.status_code == 204:
                sys.exit()
        else:
            return pathlib.Path("./tasks").absolute()
    
    def get_contacts(
            self,
            orgId: Optional[str] = None,
            domain: Optional[str] = None,
            start_date: str = "",
            upload: bool = True,
    ) -> None | pathlib.Path:
        token = self.__get_token()

        if orgId is None:
            orgId = self.__org_id
        
        if domain is None:
            domain = "contacts"

        if start_date != "":
            # validating the date passed as parameter
            valid_date = re.match(self.__date_pattern, start_date)

            if bool(valid_date) is False:
                raise ValueError(
                    """Invalid date!. Expected format is 
                    yyyy-MM-dd'T'HH:mm:ss.SSS'Z' wihtout the quotes."""
                )
        else:
            # veryfing if already exist downloaded tickets
            try:
                file: dict = read_json_file(path=pathlib.Path(f"last_{domain}.json").absolute())

                start_date = file.get(f"last_{domain}_downloaded_date")
            except FileNotFoundError:
                start_date = "2018-01-01T00:00:00.000Z"
        
        today = datetime.today()
        full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

        parameter: str = f"search?modifiedTimeRange={start_date},{full_last_hour_today}"
        sort_by: str = "modifiedTime"
        endpoint: str = "contacts"
        start: int = 0

        for num in range(start, start + 10_000, 100):
            response = req.get(
                url=f"{self.base_url}/{endpoint}/{parameter}&from={num}&limit=100&sortBy={sort_by}",
                headers={
                    "Authorization": f"Zoho-oauthtoken {token}",
                    "orgId": orgId
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                init: str = data[0]["modifiedTime"]
                final: str = data[-1]["modifiedTime"]
                
                init_replaces: str = init.replace(':', '-').replace('T', '_').replace('.000Z', '')
                final_replaces: str = final.replace(':', '-').replace('T', '_').replace('.000Z', '')

                write_json_file(
                    path=f"./{domain}",
                    file_name=f"{domain}_from_{init_replaces}_to_{final_replaces}",
                    data=data
                )

                write_json_file(
                    file_name=f"last_{domain}",
                    data={
                        f"last_{domain}_downloaded_date": f"{final}"
                    },
                    log_event=False
                )
            elif response.status_code == 204:
                break
            else:
                pass
        
        saved_files_path = pathlib.Path(f"./{domain}").absolute()

        if upload:
            send_data_to_s3(
                saved_files_path,
                bucket="501464632998-prod-landing-corporate",
                key=f"zohodesk/{domain}"
            )

            if response.status_code == 204:
                sys.exit()
        else:
            return saved_files_path

    def get_api_data(
            self,
            domain: Literal["contacts", "tasks", "tickets"],
            orgId: Optional[str] = None,
            upload: Optional[bool] = True,
            from_beggining: Optional[bool] = False
    ) -> None | pathlib.Path:
        token = self.__get_token()
        orgId = self.__org_id if orgId is None else orgId

        start_date = get_infos(f"{domain}_last_downloaded_date")

        if from_beggining or start_date is None:
            start_date = "2015-01-01T00:00:00.000Z"
        
        today = datetime.today()
        full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

        parameter: str = f"search?modifiedTimeRange={start_date},{full_last_hour_today}&"
        sort_by: str = "modifiedTime"
        endpoint: str = "contacts"
        num: int = 0

        while True:
            response = req.get(
                url=f"{self.base_url}/{endpoint}/{parameter}from=0&limit=100&sortBy={sort_by}",
                headers={
                    "Authorization": f"Zoho-oauthtoken {token}",
                    "orgId": orgId
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                init: str = data[0]["modifiedTime"]
                final: str = data[-1]["modifiedTime"]
                
                init_replaces: str = init.replace(':', '-').replace('T', '_').replace('.000Z', '')
                final_replaces: str = final.replace(':', '-').replace('T', '_').replace('.000Z', '')

                write_json_file(
                    path=f"./{domain}",
                    file_name=f"{domain}_from_{init_replaces}__to__{final_replaces}",
                    data=data
                )

                update_infos(key=f"{domain}_last_downloaded_date", value=f"{final}")
            elif response.status_code == 204:
                if upload:
                    send_data_to_s3(
                        pathlib.Path(f"./{domain}").absolute(),
                        bucket="501464632998-prod-landing-corporate",
                        key=f"zohodesk/{domain}"
                    )
                
                break

            num += 100
            parameter: str = f"search?modifiedTimeRange={final},{full_last_hour_today}&"

            if num == 5_000 and upload:
                send_data_to_s3(
                    pathlib.Path(f"./{domain}").absolute(),
                    bucket="501464632998-prod-landing-corporate",
                    key=f"zohodesk/{domain}"
                )

                num = 0





    def get_ticket_history(self, ticket_id: str):
        token = self.__get_token()
        response = req.get(
            url=f"{self.base_url}/tickets/{ticket_id}/History",
            headers={
                "Authorization": f"Zoho-oauthtoken {token}",
                "orgId": self.__org_id
            }
        )

        if response.status_code == 200:
            try:
                history_data = json.loads(response.content)
                logging.info(f"Successfully fetched history for ticket {ticket_id}.")
                return history_data
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON response for ticket {ticket_id}: {e}")
                return None
        else:
            logging.error(f"Failed to fetch history for ticket {ticket_id}. Status Code: {response.status_code}")
            return None


    def get_ticket_ids_from_parquet(self, bucket: str, prefix: str):
        s3_path = f"s3://{bucket}/{prefix}year=*/month=*/day=*/"
        fs = s3fs.S3FileSystem()

        parquet_files = fs.glob(f"{s3_path}*.parquet")

        if not parquet_files:
            logging.error("Nenhum arquivo Parquet encontrado no S3.")
            return []

        '''
        df = pd.read_parquet(fs.open(parquet_files[0], 'rb'), engine='fastparquet')
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print("**************** START SCHEMA ****************")
        print(df.dtypes)
        print("**************** END SCHEMA ****************")
        sys.exit()
        '''

        ticket_ids = []
        for file in parquet_files:
            df = pd.read_parquet(fs.open(file, 'rb'), engine='fastparquet')
            if 'id' in df.columns:
                ticket_ids.extend(df['id'].tolist())

        return ticket_ids


    def process_ticket_histories(self):
        bucket = "501464632998-prod-trusted-corporate"
        prefix = "zohodesk/tickets/"

        ticket_ids = self.get_ticket_ids_from_parquet(bucket, prefix)
        
        save_path = pathlib.Path("./ticket_histories")
        save_path.mkdir(exist_ok=True)

        for ticket_id in ticket_ids:
            history = self.get_ticket_history(ticket_id)

            if history:
                print(f"Gravando arquivo localmente: {save_path} para S3...")
                write_json_file(
                    file_name=f"ticket_{ticket_id}_history",
                    data=history,
                    path=save_path
                )
                file_path = save_path / f"ticket_{ticket_id}_history.json"
                if file_path.exists():
                    print(f"Enviando arquivo: {file_path} para S3...")
                    send_data_to_s3(path=file_path, domain="ticket_histories")
