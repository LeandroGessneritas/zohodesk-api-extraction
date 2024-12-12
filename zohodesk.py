from datetime import datetime, timezone
from typing import Literal, Optional
from utils import write_json_file
import requests as req
import logging
import pathlib
import json
import sys
import os
import re


logging.basicConfig(level=logging.INFO)


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
    
    def get_organizations(self) -> tuple[str, str]:
        token = self.__get_token()

        response = req.get(
            url=f"{self.base_url}/organizations",
            headers={
                "Authorization": f"Zoho-oauthtoken {token}"
            }
        )

        if response.status_code == 200:
            data = json.loads(response.content)['data'][0]

            return (
                data['companyName'],
                str(data['id'])
            )
    
    def get_tickets(
            self,
            orgId: str,
            credentials: Optional[dict] = None,
            save_type: Literal['local', 'cloud'] = 'local',
            save_path: Optional[str] = './tickets',
            sort_by: Literal['createdTime', 'modifiedTime'] = 'modifiedTime',
            order: Literal['ascending', 'descending'] = 'ascending',
            only_updated: bool = False,
            start_date: Optional[str] = "",
            start: int = 0,
            keep_counting: bool = False,
    ) -> pathlib.Path:
        token = self.__get_token()

        sign: str = "-" if order == "descending" else ""

        if only_updated:
            valid_date = re.match(self.__date_pattern, start_date)

            if start_date == "" or bool(valid_date) is False:
                raise ValueError(
                    """Valid date is required to get only updated tickets.
                    Expected format is yyyy-MM-dd'T'HH:mm:ss.SSS'Z' wihtout the quotes."""
                )
            
            today = datetime.today()
            full_last_hour_today = f"{today.year}-{today.month:0>2}-{today.day:0>2}T23:59:59.999Z"

            url: str = f"{self.base_url}/tickets/search?modifiedTimeRange={start_date},{full_last_hour_today}&"

            sort_by = "modifiedTime"
        else:
            # TODO
            url: str = f"{self.base_url}/tickets?"

        for num in range(start, start + 500_000, 100):
            response = req.get(
                url=f"{url}from={num}&limit=100&sortBy={sign}{sort_by}",
                headers={
                    "orgId": orgId,
                    "Authorization": f"Zoho-oauthtoken {token}"
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                final = num + 99 if len(data) == 100 else len(data) + num

                if save_type == "local":
                    if save_path is None or save_path == "":
                        raise ValueError("Path is required for local storing.")

                    if keep_counting:
                        pass
                    else:
                        write_json_file(
                            path=save_path,
                            file_name=f"tickets_from_{num}_to_{final}",
                            data=data
                        )

                    write_json_file(
                        path="./",
                        file_name="last_ticket",
                        data={"last_ticket": f"{final}"}
                    )
                elif save_type == 'cloud' and credentials is None:
                    raise ValueError("Credentials are required when saving to cloud.")
                else:
                    pass
            elif response.status_code == 204:
                break
            else:
                pass
        
        return pathlib.Path("./tickets").absolute()
