import os
from flask import Flask, Request, jsonify, request
from slack_bolt.adapter.flask import SlackRequestHandler
from main import app  

# Flask app for local testing
flask_app = Flask(__name__)
handler = SlackRequestHandler(app)  # Reuse the handler from main.py

# Decorator to inject the Request object
def inject_request(func):
    def wrapper(*args, **kwargs):
        # Pass the global request object into the function
        return func(request, *args, **kwargs)
    return wrapper

@flask_app.route("/slack/events", methods=["POST"])
@inject_request  # Inject the request object explicitly to make it look as closer to the actual function as possible
def welcome_app(req: Request):
    """Simulate the Slack event handler."""

    print("----------------------------------------------------")
    print("Received request headers:")
    print(req.headers)

    print("----------------------------------------------------")
    print("Received request body:")
    print(req.data.decode('utf-8'))


    # Handle Slack's URL verification challenge
    if req.json.get("type") == "url_verification":
        challenge = req.json.get("challenge")
        return jsonify({"challenge": challenge}), 200

    if "X-Slack-Retry-Num" in req.headers:
        return "OK", 200
    else:
        return handler.handle(req)

if __name__ == "__main__":
    flask_app.run(port=3000, debug=True)
