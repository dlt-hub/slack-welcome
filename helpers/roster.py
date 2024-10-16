import base64
import os
from datetime import datetime, timedelta, timezone
import json

from google.oauth2 import service_account
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

base64_encoded_key = base64.b64decode(os.environ["SERVICE_ACCOUNT_KEY"]).decode('utf-8')
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

people = {
   "U04DSNZTXS8": "adrian@dlthub.com",
#   "U06AF0R46E4": "akela@dlthub.com",
   "U054H6GT3BP": "alena@dlthub.com",
#    "U050C387T0D": "anton@dlthub.com",
   "U0620NFUF9V": "anuun@dlthub.com",
#    "U0562K5D2P6": "david@dlthub.com",
   "U064F5A6NKS": "hiba@dlthub.com",
#   "U07FL7WBEAY": "maria@dlthub.com",
#    "U04DQ9VUN1Y": "marcin@dlthub.com",
   "U050C2F3K29": "rahul@dlthub.com",
#    "U06AC6C5YAF": "sultan@dlthub.com",
#   "U06GPPD73AN": "violetta@dlthub.com",
#    "U05CYPLR5KJ": "zaeem@dlthub.com",
   "U07PXURC5K6": "hulma@dlthub.com",
   "U07NM2TTU3A": "nikesh@dlthub.com"
}

def is_online(service,email,start,end):

    events_result = service.events().list(
            calendarId=email,
            timeMin=start,
            timeMax=end,
            singleEvents=True,
            orderBy='startTime').execute()
    for event in events_result.get('items',[]):
        if 'outOfOffice' == event.get('eventType', ''):
            ooo_start_time = datetime.fromisoformat(event["start"]["dateTime"]).astimezone(timezone.utc)
            ooo_end_time = datetime.fromisoformat(event["end"]["dateTime"]).astimezone(timezone.utc)
            if ooo_start_time <= datetime.now(tz=timezone.utc) <= ooo_end_time:
                return False
    return True

def slack_messaging_roster():
    credentials = service_account.Credentials.from_service_account_info(
        info=json.loads(base64_encoded_key), scopes=SCOPES
    )
    service = build("calendar","v3", credentials=credentials)

    storage_client = storage.Client()
    storage_bucket = storage_client.bucket("slack-messaging-bot")
    last_person_blob = storage_bucket.blob("last_person.txt")
    last_person_tagged_index = int(last_person_blob.download_as_string())

    start_utc_time = (datetime.now() - timedelta(days=1)).isoformat() + 'Z'
    end_utc_time = (datetime.now() + timedelta(days=1)).isoformat() + 'Z'
    
    people_list = list(people.items())
    num_people = len(people_list)
    count = 0

    # Check if today is Monday (0 = Monday, 6 = Sunday) - This will be used to exclude Alena and Adrian unless it's Monday
    is_monday = datetime.now().weekday() == 0

    while(count <= num_people):
        last_person_tagged_index += 1
        count += 1
        person, email = people_list[last_person_tagged_index % num_people]

        # If it's not Monday, skip Alena and Adrian
        if not is_monday and person in ["U054H6GT3BP", "U04DSNZTXS8"]: 
            print(f"SKIPPING {person} BECAUSE IT'S NOT MONDAY")
            continue

        try:
            if is_online(service,email,start_utc_time,end_utc_time):
                print(f"ONLINE PERSON {person}")
                last_person_blob.upload_from_string(str(last_person_tagged_index))
                return person,email
        except HttpError:
            print(f"ERROR ENCOUNTERED AT PERSON {person}")
            continue
