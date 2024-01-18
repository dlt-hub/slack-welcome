import dlt
import logging
import os
import pendulum
import time

from datetime import datetime
from slack_bolt import App
from slack_sdk import WebClient
from slack_bolt.adapter.google_cloud_functions import SlackRequestHandler
from flask import Request
from google.cloud import bigquery


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

@app.event("member_joined_channel")
def member_joined_event(ack,body):
  
  channel_id = body['event']['channel']
  if channel_id == "C05CKMZSF52":
    user_id = body['event']['user']
    join_time = datetime.fromtimestamp(body['event_time'])
    user_info = botclient.users_info(user=user_id).data
    user_name = user_info['user']['real_name']
    user_email = user_info['user']['profile'].get('email',None)
    user_timezone = user_info['user']['tz']
    user_timezone_offset = user_info['user']['tz_offset']
    if user_email is not None:
        notification_text = f'{user_name}({user_email}) just joined the community!'
    else:
        notification_text = f'{user_name} just joined the community!'
    response = botclient.chat_postMessage(
        channel = "C05CKMNL4UC",
        text = notification_text
    )
    # response = botclient.chat_scheduleMessage(
    #     channel = user_id,
    #     post_at=1692885600,
    #     text = f'''Hey:wave: Welcome to our slack community! :simple_smile:\nIf you have any questions about dlt or the community, I would be happy to help you out. :left_speech_bubble:  You can also check out our <https://dlthub-community.slack.com/archives/C04DSP005D2/p1686840501177669|pinned post> on getting started for links or more info.''',
    #     unfurl_links=False
    # )
    response = botclient.chat_postMessage(
        channel = user_id,
        text = f'''Hey there! :wave: Thanks for joining the dltHub Community slack! :slightly_smiling_face: We’d love to hear more about you in <#C05QFC7BCGK|5-introduce-yourself>.\n\n*This is how you can make the most out of this community:* :rocket:\n1. :loudspeaker: Checkout <#C05CHD2ABEX|#1-announcements> for latest updates on dlt and the dltHub community.\n2. :raised_hands:Use <#C05C42YJ355|#2-sharing-and-contributing> to show off your contributions to dlt or to simply share any cool projects that you’ve built using dlt.\n3. :speech_balloon:Feel free to reach out in <#C04DQA7JJN6|#3-technical-help> for support or any technical questions related to dlt.\n4. :bulb:For all other discussions around dlt, data loading, python, or open-source, use <#C05CJEDHG2F|#4-discussions>.\n\n*Are you curious about dlt and would like to start exploring? Here are some useful resources to help get started:* :eyes:\n1. Play with the <https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing|Google Colab demo>.\n2. <https://dlthub.com/docs/reference/installation|Install> dlt and try the <https://dlthub.com/docs/getting-started|Getting Started> tutorial in your laptop with DuckDB.\n3. Select an <https://dlthub.com/docs/dlt-ecosystem/verified-sources|existing pipeline> or start <https://dlthub.com/docs/getting-started/build-a-data-pipeline|creating your own>.\n\n*Are you a data engineer that intends to deploy dlt in your company?* :brain:\n\nThen consider taking advantage of the dltHub Support Program. The program offers technical guidance and support to data engineers that intend to deploy dlt in their companies. :boom:\n\n*Checkout this <https://dlthub-community.slack.com/archives/C04DSP005D2/p1690294409398659|slack message> by @adrian for more information. Or simply join the <#C05P7NTF1AR|#onboarding> channel and someone from our team will get in touch with you.*''',
        unfurl_links=False
    )

    # event_data = [
    #     {
    #         "user_name":user_name,
    #         "user_email": user_email,
    #     }
    # ]
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
        dataset_name='rahul_slack_message_sequence_test'
    )
    bigquery_pipeline.run(event_data, table_name='message_sequence_test')

  if channel_id == "C05P7NTF1AR":
  	user_id = body['event']['user']
  	event_time = datetime.fromtimestamp(body['event_time'])
  	update_query = f"""
  		UPDATE `dlthub-analytics.rahul_slack_message_sequence_test.message_sequence_test`
  			SET onboarded=true, onboarded_on_date="{event_time}" 
  			WHERE user_id="{user_id}";
  	"""
  	query_job = bqclient.query(update_query)
  	query_job.result()

  return 'OK', 200


@app.event("message")
def member_interacted_event(ack,body):

	channel_id = body['event']['channel']
	if channel_id in ["C05D748UHEC", "C05CEAKK4CD", "C05P5DH6JCF"]: # sharing-and-contributing, technical-help, discussions
		message = body["event"]["text"]
		user_id = body["event"]["user"]
		message_time = datetime.fromtimestamp(body["event_time"])
		update_query = f"""
			UPDATE `dlthub-analytics.rahul_slack_message_sequence_test.message_sequence_test`
				SET 
                    interacted=true, 
                    last_interacted_on_channel="{channel_id}", 
                    last_interacted_on_date="{message_time}",
                    last_interacted_message="{message}"
				WHERE user_id="{user_id}";
		"""
		query_job = bqclient.query(update_query)
		query_job.result()

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
    # time.sleep(5)
    if "X-Slack-Retry-Num" in req.headers:
        logging.info("retried")
        return 'OK', 200
    else:
        return handler.handle(req)