# Zoho Desk API extraction

## Authentication
The Zoho Desk API requires a token.  
Visit [Zoho Desk API Console - OAuth](https://desk.zoho.com/DeskAPIDocument#OauthTokens) to set one.  

Create an `.env` file in the folder to save the `client_id` and `client_secret` got it in the API Console.  

The `access_token` is valid for one hour.  
To generate a token which does not expire make another request, like [here](https://desk.zoho.com/DeskAPIDocument#OauthTokens#GeneratingTokens) and change the parameters:  
1 - `code`, setting to the refresh token's value  
2 - `grant_type`, set to "refresh_token"  
(have in mind this is not the securiest option).  

For more information, access the [Zoho Desk API Documentation](https://desk.zoho.com/DeskAPIDocument)

## Organizations
The organization id is required to get another information, via API.\
To get this id, call the `get_organizations` method, available in Zohodesk class (located in `zohodesk.py` file)

## Tickets
Using the organization id, invoke the method `get_tickets`.
