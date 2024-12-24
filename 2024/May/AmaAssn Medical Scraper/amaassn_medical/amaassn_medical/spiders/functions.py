import glob
import json

import requests

# from pyairtable import Api, Workspace, models, Table

base_headers_list = ['Program ID']
table_program_list = 'Program List'
table_program_list_headers = ['Program ID', 'Program Name', 'Specialty', 'Program City, State',
                              'Type of Program', 'Number of 1st Yr Positions', 'ERAS Participation', 'NRMP match',
                              'Program URL', 'Link to Overview',
                              'Link to ProgramWorkSchedule', 'Link to FeaturesBenefits']

table_overview = 'Overview'
table_overview_headers = ['Program ID', 'Description', 'University Affiliation', 'Primary Teaching Site',
                          'Accredited Training Length (years)', 'Required Length (years)', 'Accepting Apps 2024-2025',
                          'Accepting Apps 2025-2026', 'Program Start Dates', 'ERAS Participation',
                          'Government Affiliation',
                          'Web Address', 'Video Availability', 'Program Director', 'Director Contact Information',
                          'Director Tel',
                          'Director Email', 'Contact Person', 'Contact Person Information', 'Contact Tel',
                          'Contact Email', 'Last Updated', 'Survey Received', 'Location', 'Sponsor',
                          'Participant Institution 1',
                          'Participant Institution 2', 'Participant Institution 3', 'Participant Institution 4',
                          'Participant Institution 1 URL',
                          'Participant Institution 2 URL', 'Participant Institution 3 URL',
                          'Participant Institution 4 URL', 'Link to Program List']

table_programworkschedule = 'ProgramWorkSchedule'

table_featuresbenefits = 'FeaturesBenefits'


def read_user_cred_from_input_file():
    """
    Reads a text file and returns its contents as a dictionary.
    :return: Dictionary containing the text file data.
    """
    file_path = ''.join(glob.glob('input/user_cred.txt'))
    data = {}
    with open(file_path, mode='r', encoding='utf-8-sig') as text_file:
        for line in text_file:
            key, value = line.strip().split('==', maxsplit=1)
            data[key.strip()] = value.strip()
    return data


def get_university_data(detail_div, data):
    pattern = rf'.*?{data}.*'
    return detail_div.css('.ng-star-inserted::text').re_first(pattern)


""" Upload Data At Airtable """


def upload_airtable_data(data):
    airtable_headers = {
        'Authorization': f'Bearer {data.api_token}',
        'Content-Type': 'application/json',
    }
    Api = ''
    api = Api(data.api_token)

    # Get user info
    print('User INfo Request made')
    user_info = api.whoami()
    print(f" User information : {user_info}")

    # All Bases Info
    bases_info = {}
    bases = api.bases()
    for base in bases:
        bases_info[base.name] = base.id
    print(f"All Bases in the workspace are : {bases_info}")

    # if NO base Found then create new one
    if not bases_info:
        print('No Base exist request to create new one Base')
        url = 'https://api.airtable.com/v0/meta/bases'
        create_base = requests.request(method='POST', url=url, headers=airtable_headers,
                                       data=json.dumps(
                                           base_headers(table_name=table_program_list, workspace_id=data.workspace_id,
                                                        headers=table_program_list_headers)))

        if create_base.status_code == 200:
            req = requests.request(method='Get', url='https://api.airtable.com/v0/meta/bases', headers=airtable_headers)
            bases = req.json().get('bases', [])
            for base in bases:
                bases_info[base.get('name')] = base.get('id')
            print(f"New BAse create in the workspace is : {bases_info}")

    data.mandatory_logs.append(f"Already Bases Exist {bases_info}")

    ########################################
    """tables records in the specific Base """
    if bases_info:
        # for base_name, base_id in bases_info.items():
        base_name, base_id = next(iter(bases_info.items()))
        print(f"Request made for getting information already exists tables on the Base: {base_name} ")

        url = f'https://api.airtable.com/v0/meta/bases/{base_id}/tables'
        base_detail = requests.get(url, headers=airtable_headers)
        tables_info = {}
        if base_detail.status_code == 200:
            for table in base_detail.json().get('tables', [{}]):
                tables_info[table.get('name', '')] = table.get('id', '')

        print(f" tables on the Base: {base_name} are : {tables_info} ")

        data.mandatory_logs.append(f"Bases Has Tables: {tables_info}")

    ########################################
    """program_list Table Functionalities"""
    # Use First Base:
    base_name, base_id = next(iter(bases_info.items()))
    create_table_url = f'https://api.airtable.com/v0/meta/bases/{base_id}/tables'

    # Create program_list Table
    create_table_req = requests.request(method='POST', url=create_table_url, headers=airtable_headers,
                                        data=json.dumps(create_table_data_headers(table_name=table_program_list,
                                                                                  headers=table_program_list_headers)))
    table_program_list_previous_records = []
    if create_table_req.status_code == 200:
        print("Table created successfully")
    elif 'DUPLICATE_TABLE_NAME' in str(create_table_req.content):
        print(f"Table Already exist")
        table_program_list_previous_records = get_previous_records_airtable(api, base_id, table_name=table_program_list)
    else:
        print(f"Error: {create_table_req.content}")
        data.error.append(
            f"Table Name {table_program_list} Not created Successfully Error: {json.loads(create_table_req.content).get('error', {}).get('message', '')}")

    # Step 2: Insert records into the table
    insert_records_airtable(current_records=data.table_program_list_records, table_name=table_program_list,
                            previous_program_ids=table_program_list_previous_records, base_id=base_id,
                            headers=airtable_headers)

    ########################################
    """Overview Table Functionalities"""
    create_table_req = requests.request(method='POST', url=create_table_url, headers=airtable_headers,
                                        data=json.dumps(create_table_data_headers(table_name=table_overview, headers=table_overview_headers)))

    table_overview_previous_records_ids = []
    if create_table_req.status_code == 200:
        print("Table created successfully")
    else:
        print(f"Error: {create_table_req.content}")
        table_overview_previous_records_ids = get_previous_records_airtable(api, base_id, table_name=table_overview)

    # Step 2: Insert records into the table
    insert_records_airtable(current_records=data.table_overview_records, table_name=table_overview,
                            previous_program_ids=table_overview_previous_records_ids, base_id=base_id, headers=table_overview_headers)

    a = 1


def get_user_cred(self):
    credentials = {}
    with open('input/user_credentials.txt', mode='r', encoding='utf-8') as txt_file:
        for line in txt_file:
            key, value = line.strip().split('==')
            credentials[key.strip()] = value.strip()
    return credentials


def create_table_data_headers(table_name, headers):
    field_definitions = {}

    if table_name == 'Program List':
        field_definitions = {
            # Program list table headers
            'Program ID': {'type': 'number', 'options': {'precision': 0}},
            'Program Name': {'type': 'singleLineText'},
            'Specialty': {'type': 'singleLineText'},
            'Program City, State': {'type': 'singleLineText'},
            'Type of Program': {'type': 'singleLineText'},
            'Number of 1st Yr Positions': {'type': 'singleLineText'},
            # 'ERAS Participation': {'type': 'singleLineText'},
            'ERAS Participation': {'type': 'checkbox', 'options': {'color': 'greenBright', 'icon': 'check'}},
            # 'NRMP match': {'type': 'singleLineText'},
            'NRMP match': {'type': 'checkbox', 'options': {'color': 'greenBright', 'icon': 'check'}},
            'Program URL': {'type': 'url'},
            'Link to Overview': {'type': 'url'},
            'Link to ProgramWorkSchedule': {'type': 'url'},
            'Link to FeaturesBenefits': {'type': 'url'},
        }
    elif table_name == 'Overview':
        # Table overview Headers
        field_definitions = {
            'Program ID': {'type': 'number', 'options': {'precision': 0}},
            'Description': {'type': 'singleLineText'},
            'University Affiliation': {'type': 'singleLineText'},
            'Primary Teaching Site': {'type': 'singleLineText'},
            'Accredited Training Length (years)': {'type': 'number', 'options': {'precision': 0}},
            'Required Length (years)': {'type': 'number', 'options': {'precision': 0}},
            'Accepting Apps 2024-2025': {'type': 'singleLineText'},
            'Accepting Apps 2025-2026': {'type': 'singleLineText'},
            'Program Start Dates': {'type': 'singleLineText'},
            'ERAS Participation': {'type': 'checkbox', 'options': {'color': 'greenBright', 'icon': 'check'}},
            'Government Affiliation': {'type': 'singleLineText'},
            'Web Address': {'type': 'url'},
            'Video Availability': {'type': 'singleLineText'},
            'Program Director': {'type': 'singleLineText'},
            'Director Contact Information': {'type': 'richText'},
            'Director Tel.': {'type': 'singleLineText'},
            'Director Email': {'type': 'singleLineText'},
            'Contact Person': {'type': 'singleLineText'},
            'Contact Person Information': {'type': 'singleLineText'},
            'Contact Tel.': {'type': 'singleLineText'},
            'Contact Email': {'type': 'singleLineText'},
            'Last Updated': {'type': 'date', 'options': {'dateFormat': {'format': 'M/D/YYYY', 'name': 'us'}}},
            'Survey Received': {'type': 'date', 'options': {'dateFormat': {'format': 'M/D/YYYY', 'name': 'us'}}},
            'Location': {'type': 'richText'},
            # 'Sponsor': {'type': 'singleLineText'},
            'Sponsor': {'type': 'singleLineText'},
            # 'Participant Institution 1': {'type': 'multiLineText'},
            'Participant Institution 1': {'type': 'richText'},
            'Participant Institution 2': {'type': 'richText'},
            'Participant Institution 3': {'type': 'richText'},
            'Participant Institution 4': {'type': 'richText'},
            'Participant Institution 1 URL': {'type': 'url'},
            'Participant Institution 2 URL': {'type': 'url'},
            'Participant Institution 3 URL': {'type': 'url'},
            'Participant Institution 4 URL': {'type': 'url'},
            'Link to Program List': {'type': 'url'},
        }

    # Create the fields list based on the headers and their definitions
    fields = []
    for header in headers:
        if header in field_definitions:
            field = {'name': header, **field_definitions[header]}
        else:
            field = {'name': header, 'type': 'singleLineText'}
        fields.append(field)

    create_table_data = {
        'description': 'All records from the University',
        'fields': fields,
        'name': table_name
    }

    return create_table_data


def get_previous_records_airtable(api, base_id, table_name):
    print('Request for previous Records in the table : ', table_name)
    table = api.table(base_id, table_name)
    all_records = table.all()
    previous_program_ids = [record.get('fields', {}).get('Program ID', 0) for record in all_records]
    previous_program_ids = [str(pid) for pid in previous_program_ids]

    return previous_program_ids


def insert_records_airtable(table_name, previous_program_ids, current_records, base_id, headers):
    insert_records_url = f'https://api.airtable.com/v0/{base_id}/{table_name}/'

    # check the current records already exist on airtable or not
    insert_records_ids = [row for row in current_records if
                          row.get('Program ID', '').lstrip('0') not in previous_program_ids]

    if insert_records_ids is None:
        return

    # Convert boolean values to tick/cross signs
    for record in insert_records_ids:
        for key, value in record.items():
            if key == 'Program ID':
                record[key] = int(value)
            # if key == 'ERAS Participation' and str(value) == 'True' or key == 'NRMP match' and str(value) == 'True':
            #     record[key] = '✓'
            elif key == 'ERAS Participation' and str(value) == 'False' or key == 'NRMP match' and str(value) == 'False':
                record[key] = ''
            elif 'type of program' in key.lower() and value == '' or 'number of 1st yr positions' in key.lower() and value == '':
                record[key] = 'N/A'

    # limit of upload records are 10 so make batches of 10 records per batch
    batches = [insert_records_ids[i:i + 10] for i in range(0, len(insert_records_ids), 10)]
    print('Total Records Uploading :', len(insert_records_ids))

    for batch in batches:
        insert_records_data = {"records": [{"fields": record} for record in batch]}

        insert_records_req = requests.request(method='POST', url=insert_records_url, headers=headers,
                                              data=json.dumps(insert_records_data))
        if insert_records_req.status_code == 200:
            print(f"{len(insert_records_data.get('records'))} :Records inserted successfully")
        else:
            print(f"Error: {insert_records_req.content}")

    return None


def base_headers(table_name, workspace_id, headers):
    field_definitions = {
        'Program ID': {'type': 'number', 'options': {'precision': 0}},
        'Program Name': {'type': 'singleLineText'},
        'Specialty': {'type': 'singleLineText'},
        'Program City, State': {'type': 'singleLineText'},
        'Type of Program': {'type': 'singleLineText'},
        'Number of 1st Yr Positions': {'type': 'singleLineText'},
        # 'ERAS Participation': {'type': 'checkbox'},
        'NRMP match': {'type': 'singleLineText'},
        'Program URL': {'type': 'url'},
        'Link to Overview': {'type': 'url'},
        'Link to ProgramWorkSchedule': {'type': 'url'},
        'Link to FeaturesBenefits': {'type': 'url'},
    }

    fields = []
    for header in headers:
        if header in field_definitions:
            field = {"name": header, **field_definitions[header]}
        else:
            field = {"name": header, "type": "singleLineText"}
        fields.append(field)

    data = {
        'name': 'Base Freida Records',
        "tables": [
            {
                "description": "Main Programs of Universities",
                "fields": fields,
                'name': table_name
            }
        ],
        "workspaceId": workspace_id
    }

    return data
