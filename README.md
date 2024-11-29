## Authentication
To use the Zoho Desk API it is needed a token.  
To get this token it is needed access the [Zoho Desk API Console](https://api-console.zoho.com/).  
After you're in, you'll see the page bellow.  

![zoho-api-console-initial-page](https://github.com/user-attachments/assets/805a3d3c-dc5d-4324-83f2-4e78bb1137fc)  

Click on "Get Started" and choose the client type you want.  

![image](https://github.com/user-attachments/assets/dda02921-e23e-4103-8609-b58e83a5dc04)


In my case, I've chosen "Self Client" and I see this page:  

![image](https://github.com/user-attachments/assets/49e8354c-bd9d-41b5-b724-be22e9368479)

You'll be asked if you really want to continue:  

![image](https://github.com/user-attachments/assets/7adb2df8-b413-4eb4-90b0-33cb90b64926)

Once you confirm, it will be shown your Client ID and Client Secret codes:  

![Screenshot_2](https://github.com/user-attachments/assets/06daf584-1f15-4a34-b783-9c3b47c668f7)  

Copy those informations and save in an `.env` file, in the same folder the `main.py` file.  

Now you have to define the OAuth Scopes (read, write, create, etc).

---

This README documentation is based in the official documentation.  
To see everything in here and for more information, access the [Zoho Desk API Documentation](https://desk.zoho.com/DeskAPIDocument)
