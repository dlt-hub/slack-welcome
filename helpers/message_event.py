import dlt


from datetime import datetime
from google.cloud import bigquery

import asyncio

from helpers.enrichment import enrich_user
from helpers.dhelp import ask_dhelp
from helpers.roster import slack_messaging_roster

def handle_message_event(ack, body, botclient, bqclient):
    """
    Handles the "message" Slack event when a user sends a message to a set of channels.
    Specifically, it processes messages sent to #technical-help and #anuuns-debug-channel.
    Additionally, it updates internal data using messages sent to
    #sharing-and-contributing, #technical-help, #discussions, #introduce-yourself and #anuuns-debug-channel.
    """

    channel_id = body['event']['channel']

    if channel_id == "C04DQA7JJN6" or channel_id == "C07R1362X0D": #technical-help channel + #anuuns-debug-channel channel
        message_text = body['event']['text']
        message_ts = body['event'].get('thread_ts', None)

        # If message_ts is None then the message is not part of a thread, but rather an original message
        if message_ts is None:
            message_ts = body['event']['ts']

        # Get replies to the message
        replies_response = botclient.conversations_replies(channel=channel_id,ts=message_ts)
        try:
            replies_list = replies_response.get("messages")[0]["reply_users"]
        except KeyError:
            replies_list = []

        # If the bot hasn't replied yet, make it reply
        bot_id = "U05DLSMH6KA"

        if not bot_id in replies_list:

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

            # Tag a student in community support channel (incl. Alena and Adrian on Mondays)
            id_and_email = slack_messaging_roster()
            if id_and_email is not None:
                employee_id = id_and_email[0]
                message_link = f"https://dlthub-community.slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}"
                botclient.chat_postMessage(
                     channel="C06R5BSRWRH", #community-spport channel
                     text=f"Hey <@{employee_id}>, you might want to reply to <{message_link}|this message>..."
                )

                
    if channel_id in ["C05C42YJ355", "C04DQA7JJN6", "C05CJEDHG2F", "C05QFC7BCGK", "C07R1362X0D"]: # sharing-and-contributing, technical-help, discussions, introduce-yourself, anuuns-debug-channel
        message_text = body['event']['text']
        user_id = body["event"]["user"]
        message_time = datetime.fromtimestamp(body["event_time"]).isoformat()
        user_info = botclient.users_info(user=user_id).data
        user_name = user_info['user']['real_name']
        user_firstname = user_info['user']['profile']['first_name']
        user_lastname = user_info['user']['profile']['last_name']
        user_email = user_info['user']['profile'].get('email',None)
        user_message_data = {
            "slack_user_id": user_id,
            "slack_user_name": user_name,
            "slack_user_firstname": user_firstname,
            "slack_user_lastname": user_lastname,
            "slack_user_email": user_email,
            "inserted_at_time": message_time
        }

        # Use query parameters gracefully handle strings and update the last interacted into in slack_member_join_data
        query_parameters = [
            bigquery.ScalarQueryParameter("channel_id", "STRING", channel_id),
            bigquery.ScalarQueryParameter("message_time", "STRING", message_time),
            bigquery.ScalarQueryParameter("message_text", "STRING", message_text),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]

        update_query = f"""
            UPDATE `dlthub-analytics.slack_member_join_data.new_member_info`
                SET 
                    interacted=true, 
                    last_interacted_on_channel=@channel_id, 
                    last_interacted_on_date=@message_time,
                    last_interacted_message=@message_text
                WHERE user_id=@user_id;
        """

        interacted_write_query_job = bqclient.query(update_query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
        interacted_write_query_job.result() # This ensures the query has completed

        # If the user's enriched data isn't in rahul_enriched_users, then add it
        query = """
            SELECT DISTINCT(slack_user_id)
                FROM `dlthub-analytics.rahul_enriched_users.enriched_users`
        """
        enrichment_write_query_job = bqclient.query(query)
        enrichment_write_query_job.result() # This ensures the query has completed
        existing_users = [row['slack_user_id'] for row in enrichment_write_query_job]

        if user_id not in existing_users:

            enrichment_response = enrich_user(
                                first_name=user_firstname,
                                last_name=user_lastname,
                                email=user_email
                            )
            
            enrichment_data = [{**user_message_data, **enrichment_response}]

            enrichment_pipeline = dlt.pipeline(
                pipeline_name="user_enrichment",
                destination="bigquery",
                dataset_name="rahul_enriched_users"
            )

            enrichment_pipeline.run([enrichment_data], table_name="enriched_users")

    return 'OK', 200
