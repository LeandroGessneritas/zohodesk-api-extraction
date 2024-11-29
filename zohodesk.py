from dotenv import load_dotenv
from typing import Literal
import requests as req
import logging
import boto3
import json
import os


load_dotenv()


class Zohodesk():
    def __init__(
            self,
            code: str | None = None
    ) -> None:
        self.base_url: str = "https://desk.zoho.com/api/v1"
        self.token_url: str = "https://accounts.zoho.com/oauth/v2/token"
        self.__client_id: str = os.getenv("CLIENT_ID")
        self.__client_secret: str = os.getenv("CLIENT_SECRET")
        self.code: str = code

    def __write_json_file(
            self,
            file_name: str,
            data: dict,
            mode: str = "w",
    ) -> None:
        # dictObj: dict = {}

        # if mode == "a":
        #     with open(f"{file_name}.json", "r", encoding='utf-8') as file:
        #         dictObj = json.load(file)
            
        #     dictObj.update(data)
        #     mode = "w"
        
        # data = dictObj if dictObj else data
        
        with open(f"{file_name}.json", mode, encoding="utf-8") as file:
            file.write(json.dumps(data, indent=4, ensure_ascii=True))

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

        self.__write_json_file(
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

    def get_users(
            self,
            orgId: str
    ):
        response = req.get(
            url=f"{self.base_url}/users",
            headers={
                "orgId": orgId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def get_groups(
            self,
            orgId: str
    ):
        response = req.get(
            url=f"{self.base_url}/groups",
            headers={
                "orgId": orgId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def get_group_details(
            self,
            orgId: str,
            groupId: str
    ):
        response = req.get(
            url=f"{self.base_url}/groups/{groupId}",
            headers={
                "orgId": orgId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def get_tickets(
            self,
            orgId: str,
            save: Literal['local', 'cloud'] = 'local'
    ) -> None:
        token = self.__get_token()

        for num in range(0, 500_000, 100):
            response = req.get(
                url=f"{self.base_url}/tickets?from={num}&limit=100&sortBy=createdTime",
                headers={
                    "orgId": orgId,
                    "Authorization": f"Zoho-oauthtoken {token}"
                }
            )

            if response.status_code == 200:
                data = json.loads(response.content)['data']

                final = num + 99 if len(data) == 100 else len(data) + num

                if save == "local":
                    self.__write_json_file(
                        file_name=f"tickets_from_{num}_to_{final}",
                        data=data
                    )

                    self.__write_json_file(
                        file_name="last_ticket",
                        # mode="a",
                        data={"last_ticket": f"{final}"}
                    )
                elif save == 'cloud':
                    pass
            elif response.status_code == 204:
                break
            else:
                pass
    
    def get_ticket_metrics(
            self,
            orgId: str,
            ticketId: str
    ):
        response = req.get(
            url=f"{self.base_url}/tickets/{ticketId}/metrics",
            headers={
                "orgId": orgId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def get_ticket_tags(
            self,
            orgId: str,
            departmentId: str
    ):
        response = req.get(
            url=f"{self.base_url}/ticketTags",
            headers={
                "orgId": orgId,
                "departmentId": departmentId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def get_departments(
            self,
            orgId: str
    ):
        response = req.get(
            url=f"{self.base_url}/departments?isEnabled=true&chatStatus=AVAILABLE",
            headers={
                "orgId": orgId,
                "Authorization": f"Zoho-oauthtoken {self.__get_token()}"
            }
        )

        if response.status_code == 200:
            return json.loads(response.content)['data']
    
    def upload_to_s3(
            self,
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


if __name__ == "__main__":
    code = ""

    zoho = Zohodesk(code)

    org_name, org_id = zoho.get_organizations()

    zoho.get_tickets(orgId=org_id)
