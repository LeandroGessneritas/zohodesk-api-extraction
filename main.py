from zohodesk import Zohodesk


if __name__ == "__main__":
    code: str = ""

    zoho = Zohodesk(code)

    org_name, org_id = zoho.get_organizations()

    users = zoho.get_users(org_id)

    groups = zoho.get_groups(orgId=org_id)

    zoho.get_tickets(orgId=org_id)

    departments = zoho.get_departments(orgId=org_id)

    ticket_tags = zoho.get_ticket_tags(orgId=org_id)
