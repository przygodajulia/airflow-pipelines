from slack import WebClient
from slack.errors import SlackApiError
from airflow.models import Variable
import logging

logging.basicConfig(level=logging.INFO)

# get form secrets backend
slack_token = Variable.get("slack_token")
logging.info(f"Connection used {slack_token}")
client = WebClient(token=slack_token)

try:
	# Channel id is specified in channel parameter
	response = client.chat_postMessage(channel="C04JDA7GGCS", text="Hello from your app! :tada:")
except SlackApiError as e:
	assert e.response["error"]
