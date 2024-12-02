from zohodesk import Zohodesk


if __name__ == "__main__":
    code: str = ""

    zoho = Zohodesk(code)

    org_name, org_id = zoho.get_organizations()

    zoho.get_tickets(orgId=org_id)
