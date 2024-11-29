# Zoho Desk API extraction

## Authentication
The Zoho Desk API requires a token.  
Visit [Zoho Desk API Console](https://api-console.zoho.com/) to set one.  


### Get Started
![zoho-api-console-initial-page](https://github.com/user-attachments/assets/805a3d3c-dc5d-4324-83f2-4e78bb1137fc)  

Click on "Get Started" and choose the client type you want.  

![image](https://github.com/user-attachments/assets/dda02921-e23e-4103-8609-b58e83a5dc04)

In my case, I've chosen "Self Client" and I saw this page:  

![image](https://github.com/user-attachments/assets/49e8354c-bd9d-41b5-b724-be22e9368479)

You'll be asked if you really want to continue:  

![image](https://github.com/user-attachments/assets/7adb2df8-b413-4eb4-90b0-33cb90b64926)

Once you confirm, it will be shown your Client ID and Client Secret codes:

![Screenshot_2](https://github.com/user-attachments/assets/06daf584-1f15-4a34-b783-9c3b47c668f7)  

Copy that information and save it in a `.env` file, at the same folder where `main.py` file is.

Now you have to define the OAuth Scopes (read, write, create, etc).  

![image](https://github.com/user-attachments/assets/680e10c3-569a-4688-8675-589f0042785a)  
For more information about scopes, see [this codumentation](https://desk.zoho.com/DeskAPIDocument#OauthTokens#OAuthScopes)  

After this you will have to select the portal and the enviroment (prod or dev) and then click on "Create" to get the code.

Now you can get the `access_token` and the `refresh_token`.  An example using Python:  
```
response = req.post(  
    url="https://accounts.zoho.com/oauth/v2/token",
    params={
        "code": <your_code>,
        "client_id": <your_client_id>,
        "client_secret": <your_client_secret>,
        "grant_type": "authorization_code"
    }
)
```
The response's value (if the request was succesfull):  
```
{
    "access_token": "1000.39081b570de59e7469dbe...",
    "refresh_token": "1000.4d2d0433036e8ca148f602b...",
    "scope": "Desk.tickets.READ Desk.contacts.READ ...",
    "api_domain": "https://www.zohoapis.com",
    "token_type": "Bearer",
    "expires_in": 3600
}
```
You can use the `access_token` to make your requests, tha is useful for one hour  
or you can request other token that does not expire (have in mind this is not the srcuriest option).

---

This README documentation is based in the official documentation.  
To see everything in here and for more information, access the [Zoho Desk API Documentation](https://desk.zoho.com/DeskAPIDocument)
