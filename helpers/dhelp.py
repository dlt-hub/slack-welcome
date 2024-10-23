import json
import re

import websockets


def convert_markdown_links_to_slack_format(text):
    text = re.sub(r"\*\*([^\*]+)\*\*", r"*\1*", text)
    pattern = r"\[([^\]]+)\]\((https?://[^\)]+)\)"
    return re.sub(pattern, r"<\2|\1>", text)


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
