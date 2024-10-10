import base64
import dlt
import json
import logging
import os
import pendulum
import random
import requests
import time

from datetime import datetime, timedelta, timezone
from slack_bolt import App
from slack_sdk import WebClient
from slack_bolt.adapter.google_cloud_functions import SlackRequestHandler
from flask import Request
from google.cloud import bigquery, storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

import asyncio
import websockets
import re

logging.basicConfig(level=logging.DEBUG)

botclient = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
userclient = WebClient(token=os.environ["SLACK_USER_TOKEN"])
app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
    process_before_response=True
    )
handler = SlackRequestHandler(app)

bqclient = bigquery.Client()

apollo_api_key = os.environ["APOLLO_API_KEY"]
enrichment_url = "https://api.apollo.io/v1/people/match"
enrichment_request_headers = {
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
    'X-Api-Key': apollo_api_key
}

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

def enrich_user(first_name,last_name,email):
    enrichment_input = {
        # "api_key": apollo_api_key,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "reveal_personal_emails": True
    }
    enrichment_response = requests.request(
        "POST", 
        url=enrichment_url, 
        headers=enrichment_request_headers,
        json=enrichment_input
    ).json()

    return enrichment_response


@app.event("member_joined_channel")
def notify(ack,body):
  
    channel_id = body['event']['channel']
    if channel_id == "C04DSP005D2":
        user_id = body['event']['user']
        join_time = datetime.fromtimestamp(body['event_time'])        
        user_info = botclient.users_info(user=user_id).data
        user_name = user_info['user']['real_name']
        user_firstname = user_info['user']['profile']['first_name']
        user_lastname = user_info['user']['profile']['last_name']
        user_email = user_info['user']['profile'].get('email',None)
        user_timezone = user_info['user']['tz']
        user_timezone_offset = user_info['user']['tz_offset']

        if user_email is not None:
            notification_text = f'{user_name}({user_email}) just joined the community!'
        else:
            notification_text = f'{user_name} just joined the community!'
        response = botclient.chat_postMessage(
            channel = "C05DVASKFDW",
            text = notification_text
        )
        response = botclient.chat_postMessage(
            channel = user_id,
            text = f'''Hey there! :wave: Thanks for joining the dltHub Community slack! :slightly_smiling_face: We’d love to hear more about you in <#C05QFC7BCGK|5-introduce-yourself>.\n\n*This is how you can make the most out of this community:* :rocket:\n1. :loudspeaker: Checkout <#C05CHD2ABEX|1-announcements> for latest updates on dlt and the dltHub community.\n2. :raised_hands:Use <#C05C42YJ355|2-sharing-and-contributing> to show off your contributions to dlt or to simply share any cool projects that you’ve built using dlt.\n3. :speech_balloon:Feel free to reach out in <#C04DQA7JJN6|3-technical-help> for support or any technical questions related to dlt.\n4. :bulb:For all other discussions around dlt, data loading, python, or open-source, use <#C05CJEDHG2F|4-discussions>.\n\nRemember to check out our <https://dlthub.notion.site/dltHub-Slack-Support-policy-1039fb8e23cf802bae08edc87bb99a5b|support policy> and <https://dlthub.notion.site/DLTHub-Community-Slack-Channel-Community-Guidelines-b7d6301318b545f699c2f8edc001585e?pvs=4|community guidelines>.\n\nQuick Links:\n\n1. *If you're looking to start exploring dlt, check out our <https://dlthub.com/docs/|getting started page on the docs>.*\n2. *If you're interested in dedicated support for your company, please reach out through <https://dlthub.com/contact-sales|our website>.*''',
            unfurl_links=False
        )

        event_data = [
            {
                "user_id": user_id,
                "user_name": user_name,
                "user_email": user_email,
                "join_time": join_time,
                "timezone": user_timezone,
                "timezone_offset": user_timezone_offset,
                "message_sequence": 1,
                "last_message_on_date": join_time,
                "onboarded": False,
                "onboarded_on_date": datetime.fromtimestamp(0),
                "interacted": False,
                "last_interacted_on_channel": "None",
                "last_interacted_on_date": datetime.fromtimestamp(0),
                "last_interacted_message": "",
            }
        ]
        bigquery_pipeline = dlt.pipeline(
            pipeline_name='slack_to_bigquery',
            destination='bigquery',
            dataset_name='slack_member_join_data'
        )
        bigquery_pipeline.run(event_data, table_name='new_member_info')

        slack_user_data = {
            "slack_user_id": user_id,
            "slack_user_name": user_name,
            "slack_user_firstname": user_firstname,
            "slack_user_lastname": user_lastname,
            "slack_user_email": user_email,
            "slack_join_time": join_time
        }


        enriched_user_data = enrich_user(
                                first_name=user_firstname,
                                last_name=user_lastname,
                                email=user_email
                            )

        user_data = [{**slack_user_data, **enriched_user_data}]

        bigquery_pipeline = dlt.pipeline(
            pipeline_name="slack_user_upload",
            destination="bigquery",
            dataset_name="slack_user_data"
        )
        bigquery_pipeline.run(user_data, table_name="slack_users")

    if channel_id == "C05PCVB41U4":

        user_id = body['event']['user']
        user_info = botclient.users_info(user=user_id).data
        user_name = user_info['user']['real_name']
        user_email = user_info['user']['profile'].get('email',None)

        event_time = datetime.fromtimestamp(body['event_time'])
        update_query = f"""
            UPDATE `dlthub-analytics.slack_member_join_data.new_member_info`
                SET onboarded=true, onboarded_on_date="{event_time}" 
                WHERE user_id="{user_id}";
        """
        query_job = bqclient.query(update_query)
        query_job.result()

        inform_adrian = botclient.chat_postMessage(
            channel = "U04DSNZTXS8",
            text = f"Onboarding update: user {user_name}({user_email}) just joined the <#C05PCVB41U4|onboarding> channel!"
        )

    return 'OK', 200

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


def convert_markdown_links_to_slack_format(text):
    text = re.sub(r'\*\*([^\*]+)\*\*', r'*\1*', text)
    pattern = r'\[([^\]]+)\]\((https?://[^\)]+)\)'
    return re.sub(pattern, r'<\2|\1>', text)

async def ask_dhelp(message_text):
    uri = "wss://dhelp.dlthub.com/chat"  
    async with websockets.connect(uri) as websocket:

        await websocket.send(message_text)
        print(f"> Question: {message_text}")

        collected_response = ""

        # Collect streamed response
        while True:
            response = await websocket.recv()
            data = json.loads(response)

            if data.get("type") == "stream" and data.get("sender") == "bot":
                collected_response += data.get("message", "")

            if data.get("type") == "end":
                break

        print(f"> Answer: {collected_response}")

        return convert_markdown_links_to_slack_format(collected_response)

@app.event("message")
def member_interacted_event(ack,body):

    channel_id = body['event']['channel']
    message_text = body['event']['text']

    # Anuun's debug channel is the second one
    if channel_id == "C04DQA7JJN6" or channel_id == "C07R1362X0D":
        message_ts = body['event'].get('thread_ts', None)
        if message_ts is None:
            message_ts = body['event']['ts']
        threshold_ts = "1711375819.015495"
        replies_response = botclient.conversations_replies(channel=channel_id,ts=message_ts)
        try:
            replies_list = replies_response.get("messages")[0]["reply_users"]
        except KeyError:
            replies_list = []
        bot_id = "U05DLSMH6KA"
        if not bot_id in replies_list and float(message_ts) > float(threshold_ts):

            # Say thank you
            botclient.chat_postMessage(
                channel=channel_id,
                thread_ts=message_ts,
                text=f"Thank you for your question! While we try to get to as many questions as possible, we are not always able to get to them all.\n\nHere are ways you can self-help:\n\n1. Read our <https://dlthub.com/docs|extensive documentation>.\n2. Ask our dlt-aware GPT bot located at the bottom right of the docs.\n\nIf you are reporting a bug with dlt, please raise a <https://github.com/dlt-hub/dlt/issues/new/choose|GitHub issue>."
            )

            # Tell what dhelp says
            dhelp_response = asyncio.run(ask_dhelp(message_text))
            intro = (
                "\n----------------------------------\n"
                "_*:robot_face: So far, this is what our GPT bot says:*_\n\n"
            )
            disclaimer = (
                "\n\n_*:exclamation: Please note:*_ While this bot is designed to assist with information, "
                "its responses are generated based on the data it has been trained on and might not always be accurate. "
                "For critical decisions, please verify the information with official documentation or sources."
            )
            follow_up = (
                "\n\n"
                "_*:bulb: If this answer didn't help you, let us know in this thread.*_"
            )

            final_response = f"{intro}{dhelp_response}{disclaimer}{follow_up}"

            botclient.chat_postMessage(
                channel=channel_id,
                thread_ts=message_ts,
                text=final_response
            )

            # Tag a student in community support channel
            community_support = "C06R5BSRWRH"
            person = slack_messaging_roster()
            if person is not None:
                user_id = person[0]
                message_link = f"https://dlthub-community.slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}"
                botclient.chat_postMessage(
                     channel=community_support,
                     text=f"Hey <@{user_id}>, you might want to reply to <{message_link}|this message>..."
                )

#    if channel_id == "C04DQA7JJN6":
#        message_ts = body['event'].get('thread_ts', None)
#        if message_ts is None:
#            message_ts = body['event']['ts']
#        threshold_ts = "1711375819.015495"
#        replies_response = botclient.conversations_replies(channel=channel_id,ts=message_ts)
#        try:
#            replies_list = replies_response.get("messages")[0]["reply_users"]
#        except KeyError:
#            replies_list = []
#        bot_id = "U05DLSMH6KA"
#        if not bot_id in replies_list and float(message_ts) > float(threshold_ts):
            # person = slack_messaging_roster()
            # if person is not None:
            #     user_id = person[0]
            #     botclient.chat_postMessage(
            #         channel=channel_id,
            #         thread_ts=message_ts,
            #         text=f"Thank you for your question! It helps us improve our docs and product.\n<@{user_id}> will respond to you during GMT+2 working hours :raised_hands:"
            #     )
#            botclient.chat_postMessage(
#                channel=channel_id,
#                thread_ts=message_ts,
#                text=f"Thank you for your question! While we try to get to as many questions as possible, we are not always able to get to them all.\n\nHere are ways you can self-help:\n\n1. Read our <https://dlthub.com/docs|extensive documentation>.\n2. Ask our dlt-aware GPT bot located at the bottom right of the docs.\n\nIf you are reporting a bug with dlt, please raise a <https://github.com/dlt-hub/dlt/issues/new/choose|GitHub issue>."
#            )

            # Tag a student in community support channel
#            community_support = "C06R5BSRWRH"
#            person = slack_messaging_roster()
#            if person is not None:
#                user_id = person[0]
#                message_link = f"https://dlthub-community.slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}"
#                botclient.chat_postMessage(
#                     channel=community_support,
#                     text=f"Hey <@{user_id}>, you might want to reply to <{message_link}|this message>..."
#                )
                
    if channel_id in ["C05C42YJ355", "C04DQA7JJN6", "C05CJEDHG2F", "C05QFC7BCGK"]: # sharing-and-contributing, technical-help, discussions, introduce-yourself
        message = body["event"]["text"]
        user_id = body["event"]["user"]
        message_time = datetime.fromtimestamp(body["event_time"])

        # To gracefully handle strings
        query_parameters = [
            bigquery.ScalarQueryParameter("channel_id", "STRING", channel_id),
            bigquery.ScalarQueryParameter("message_time", "STRING", message_time),
            bigquery.ScalarQueryParameter("message", "STRING", message),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]

        update_query = f"""
            UPDATE `dlthub-analytics.slack_member_join_data.new_member_info`
                SET 
                    interacted=true, 
                    last_interacted_on_channel=@channel_id, 
                    last_interacted_on_date=@message_time,
                    last_interacted_message=@message
                WHERE user_id=@user_id;
        """

        interacted_write_query_job = bqclient.query(update_query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
        interacted_write_query_job.result()

        user_id = body['event']['user']
        user_info = botclient.users_info(user=user_id).data
        user_name = user_info['user']['real_name']
        user_firstname = user_info['user']['profile']['first_name']
        user_lastname = user_info['user']['profile']['last_name']
        user_email = user_info['user']['profile'].get('email',None)
        inserted_at_time = datetime.fromtimestamp(body["event_time"])
        user_data = {
            "slack_user_id": user_id,
            "slack_user_name": user_name,
            "slack_user_firstname": user_firstname,
            "slack_user_lastname": user_lastname,
            "slack_user_email": user_email,
            "inserted_at_time": inserted_at_time
        }

        query = """
            SELECT DISTINCT(slack_user_id)
                FROM `dlthub-analytics.rahul_enriched_users.enriched_users`
        """
        enrichment_write_query_job = bqclient.query(query)
        existing_users = [row['slack_user_id'] for row in enrichment_write_query_job]

        if user_id not in existing_users:

            enrichment_input = {
                "api_key": apollo_api_key,
                "first_name": user_firstname,
                "last_name": user_lastname,
                "email": user_email,
                "reveal_personal_emails": True
            }

            enrichment_response = requests.request(
                "POST", 
                url=enrichment_url, 
                headers=enrichment_request_headers,
                json=enrichment_input
            ).json()
            enrichment_data = {
                **user_data, **enrichment_response 
            }

            enrichment_pipeline = dlt.pipeline(
                pipeline_name="user_enrichment",
                destination="bigquery",
                dataset_name="rahul_enriched_users"
            )

            enrichment_pipeline.run([enrichment_data], table_name="enriched_users")

    return 'OK', 200

def welcome_app(req: Request):
    """HTTP Cloud Function.
    Args:
        req (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    if "X-Slack-Retry-Num" in req.headers:
        return 'OK', 200
    else:
        return handler.handle(req)