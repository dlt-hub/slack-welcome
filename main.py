import logging
import os

from flask import Request
from google.cloud import bigquery
from slack_bolt import App
from slack_bolt.adapter.google_cloud_functions import SlackRequestHandler
from slack_sdk import WebClient

from helpers.join_event import handle_join_event
from helpers.message_event import handle_message_event

# Setup logging
logging.basicConfig(level=logging.DEBUG)


# Slack clients
botclient = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
userclient = WebClient(token=os.environ["SLACK_USER_TOKEN"])
app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
    process_before_response=True,
)
handler = SlackRequestHandler(app)


# BigQuery client
bqclient = bigquery.Client()


# Handle joins
@app.event("member_joined_channel")
def join(ack, body):
    return handle_join_event(ack, body, botclient)


# Handle messages
@app.event("message")
def message(ack, body):
    return handle_message_event(ack, body, botclient, bqclient)


# The cloud function entry point
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
        return "OK", 200
    else:
        return handler.handle(req)
