# Community Slackbot Internals

## How it works

`main.py` contains the entry point and distributes the events to the handlers. In the `helpers` folder, there are various functionalities that are used by the handler functions. Everything is more or less well-commented, so feel free to check out the individual files if you'd like... 👀

> This repo is synced with the Cloud Function in GCP. If something gets pushed to the main branch, the Cloud Function will redeploy! Check out the  respective cloud build trigger, if you need to change something there.

## Testing
To test the Slackbot locally, follow these steps:

### 1. Prepare secrets
Since the secrets for the Cloud Function are stored in the Variables menu in the console, you'll need to set them explicitly for local testing. First, save the secret variables from GCP to an `.env` file, then save the service account file as `service_account_file.json`.

### 2. Run `test_local.py`

The `test_local.py` script sets up the local Flask server that simulates the Slackbot environment.

```python
python test_local.py
```
> You'll need to have `ngork` installed.

### 3. Set up ngrok for public access

This will create a public URL that forwards traffic to your local Flask server.

```python
ngrok http 3000
```
This will provide a public forwarding URL (e.g., https://<ngrok-id>.ngrok.io) that can be used to temporarily redirect Slack events to your local server.

### 4. Temporarily change the slack app's subscription endpoint

Under `Event Subscriptions` in your Slack app’s settings, update the Request URL to the ngrok URL:

```
https://<ngrok-id>.ngrok.io/slack/events
```

> Note: Try this during low-traffic times, such as in the evening. Since the Slack handler authenticates requests, the only way to properly test it is to use real data and receive actual requests from Slack. TLDR: Simulating requests locally won’t work.

### 5. Test it out

Now that the subscription URL is updated, you can check how messages are handled. There's no need to worry about missing data, as the GCP credentials are set, and the data is being sent to the correct destination. (But of course, double-check if you're hesitant.)

### 6. Push changes

Make a PR, merge it, and any changes to the main branch will automatically trigger the redeployment of the Cloud Function.

### 7. Set the subscription URL

After testing and redeploying, don't forget to update the subscription URL of the Slack app.