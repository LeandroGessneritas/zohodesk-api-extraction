from utils import write_json_file, read_json_file, upload_to_s3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import requests as req
import logging
import pathlib
import json
import sys
import os
import re


logging.basicConfig(level=logging.INFO)


@dataclass
class Organizations:
    companyName: str
    companyId: str


class Zohodesk:
    def __init__(
            self,
            code: str | None = None
    ) -> None:
        self.base_url: str = "https://desk.zoho.com/api/v1"
        self.token_url: str = "https://accounts.zoho.com/oauth/v2/token"
        self.__client_id: str = os.getenv("CLIENT_ID")
        self.__client_secret: str = os.getenv("CLIENT_SECRET")
        self.code: str = code
        # TODO: increase pattern for date "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        self.__date_pattern = r"2[0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z"

        if "win" in sys.platform:
            from dotenv import load_dotenv
            load_dotenv()

    def __generate_refresh_token(self) -> None:
        logging.warning("Generating refresh token...")

        if self.code is None:
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

        write_json_file(
            file_name="refresh_token",
            data={"refresh_token": json_response.get("refresh_token")}
        )

        logging.warning("DONE.")

    def __get_refresh_token(self) -> str:
        if "refresh_token.json" not in os.listdir(os.getcwd()):
            self.__generate_refresh_token()
        
        with open("refresh_token.json", "r") as file:
            return json.load(file)['refresh_token']
    
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

        return json.loads(resp.content)['access_token']
    
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
            orgId: str,
            save_path: Optional[str] = './tickets',
            start_date: str = "",
            upload: bool = False,
    ) -> pathlib.Path:
        # getting the token
        token = self.__get_token()
        
        if start_date != "":
            # validating the date passed as parameter
            valid_date = re.match(self.__date_pattern, start_date)

            if bool(valid_date) is False:
                raise ValueError(
                    """Valid date is required to get only updated tickets.
                    Expected format is yyyy-MM-dd'T'HH:mm:ss.SSS'Z' wihtout the quotes."""
                )
        
        # veryfing if already exist downloaded tickets
        if pathlib.Path(save_path).is_dir():
            try:
                file: dict = read_json_file(path=pathlib.Path("last_ticket.json").absolute())

                start_date = file.get("last_ticket_downloaded_date")
            except FileNotFoundError:
                start_date = "2018-01-01T00:00:00.000Z"
        else:
            start_date = "2018-01-01T00:00:00.000Z"
        
        today = datetime.today()
        full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

        endpoint: str = "tickets"
        parameter: str = f"search?modifiedTimeRange={start_date},{full_last_hour_today}"
        limit: int = 100
        sort_by: str = "modifiedTime"
        start: int = 0

        for num in range(start, start + 5_000, 100):
            response = req.get(
                url=f"{self.base_url}/{endpoint}/{parameter}&from={num}&limit={limit}&sortBy={sort_by}",
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
            self.__send_data_to_s3(saved_tickets_path)
        else:
            return pathlib.Path("./tickets").absolute()
    
    def __send_data_to_s3(self, path: pathlib.Path) -> None:
        for file in path.iterdir():
            upload_to_s3(
                filename=file,
                bucket=os.getenv("BUCKET"),
                key=f"{os.getenv('KEY')}/{file.name}"
            )

            pathlib.Path(file).unlink()
        
        self.get_tickets(
            orgId=self.get_organizations().companyId,
            upload=True
        )
