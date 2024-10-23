from datetime import datetime

import dlt

from helpers.enrichment import enrich_user

technical_help_channel = "C04DQA7JJN6"
getting_started_channel = "C04DSP005D2"
internal_join_notifications_channel = "C05DVASKFDW"
sharing_and_contributing_channel = "C05C42YJ355"
announcements_channel = "C05CHD2ABEX"
discussions_channel = "C05CJEDHG2F"
introduce_yourself_channel = "C05QFC7BCGK"


def handle_join_event(ack, body, botclient):
    """
    Handles the "member_joined_channel" Slack event when a user joins the specified channel.
    Specifically, it processes when a user joins the "0-getting-started" channel.
    """
    channel_id = body["event"]["channel"]

    if channel_id == getting_started_channel:
        user_id = body["event"]["user"]
        join_time = datetime.fromtimestamp(body["event_time"])
        user_info = botclient.users_info(user=user_id).data
        user_name = user_info["user"]["real_name"]
        user_firstname = user_info["user"]["profile"]["first_name"]
        user_lastname = user_info["user"]["profile"]["last_name"]
        user_email = user_info["user"]["profile"].get("email", None)
        user_timezone = user_info["user"]["tz"]
        user_timezone_offset = user_info["user"]["tz_offset"]

        # Send notification to #internal-join-notifications channel
        if user_email is not None:
            notification_text = f"{user_name} ({user_email}) just joined the community!"
        else:
            notification_text = f"{user_name} just joined the community!"

        botclient.chat_postMessage(
            channel=internal_join_notifications_channel, text=notification_text
        )

        # Send a welcome DM to the user
        welcome_text = f"""Hey there! :wave: Thanks for joining the dltHub Community slack! :slightly_smiling_face: 
We’d love to hear more about you in <#{introduce_yourself_channel}|5-introduce-yourself>.

*This is how you can make the most out of this community:* :rocket:
1. :loudspeaker: Checkout <#{announcements_channel}|1-announcements> for latest updates on dlt and the dltHub community.
2. :raised_hands: Use <#{sharing_and_contributing_channel}|2-sharing-and-contributing> to show off your contributions to dlt or to simply share any cool projects that you’ve built using dlt.
3. :speech_balloon: Feel free to reach out in <#{technical_help_channel}|3-technical-help> for support or any technical questions related to dlt.
4. :bulb: For all other discussions around dlt, data loading, python, or open-source, use <#{discussions_channel}|4-discussions>.

Remember to check out our <https://dlthub.notion.site/dltHub-Slack-Support-policy-1039fb8e23cf802bae08edc87bb99a5b|support policy> and <https://dlthub.notion.site/DLTHub-Community-Slack-Channel-Community-Guidelines-b7d6301318b545f699c2f8edc001585e?pvs=4|community guidelines>.

Quick Links:

1. *If you're looking to start exploring dlt, check out our <https://dlthub.com/docs/|getting started page on the docs>.*
2. *If you're interested in dedicated support for your company, please reach out through <https://dlthub.com/contact-sales|our website>.*"""

        botclient.chat_postMessage(
            channel=user_id, text=welcome_text, unfurl_links=False
        )

        # Load data to slack_member_join_data
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
            pipeline_name="slack_to_bigquery",
            destination="bigquery",
            dataset_name="slack_member_join_data",
        )

        bigquery_pipeline.run(event_data, table_name="new_member_info")

        # Load enriched data to slack_user_data
        user_join_data = {
            "slack_user_id": user_id,
            "slack_user_name": user_name,
            "slack_user_firstname": user_firstname,
            "slack_user_lastname": user_lastname,
            "slack_user_email": user_email,
            "slack_join_time": join_time,
        }

        enrichment_response = enrich_user(
            first_name=user_firstname, last_name=user_lastname, email=user_email
        )

        user_data = [{**user_join_data, **enrichment_response}]

        bigquery_pipeline = dlt.pipeline(
            pipeline_name="slack_user_upload",
            destination="bigquery",
            dataset_name="slack_user_data",
        )

        bigquery_pipeline.run(user_data, table_name="slack_users")

    # we no longer have an onboarding channel
    #    if channel_id == "C05PCVB41U4": #onboarding channel
    #
    #        user_id = body['event']['user']
    #        user_info = botclient.users_info(user=user_id).data
    #        user_name = user_info['user']['real_name']
    #        user_email = user_info['user']['profile'].get('email',None)

    #        event_time = datetime.fromtimestamp(body['event_time'])
    #        update_query = f"""
    #            UPDATE `dlthub-analytics.slack_member_join_data.new_member_info`
    #                SET onboarded=true, onboarded_on_date="{event_time}"
    #                WHERE user_id="{user_id}";
    #        """
    #        query_job = bqclient.query(update_query)
    #        query_job.result()

    #        inform_adrian = botclient.chat_postMessage(
    #            channel = "U04DSNZTXS8",
    #            text = f"Onboarding update: user {user_name} ({user_email}) just joined the <#C05PCVB41U4|onboarding> channel!"
    #        )

    return "OK", 200
