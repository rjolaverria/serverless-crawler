import logging
import os
import requests
from boto3 import resource
from urllib.parse import urlparse
from random import randint
from time import sleep
from utils.files import save_file, save_raw_page

PAGES_BUCKET_NAME = os.environ.get("PAGES_BUCKET_NAME")
BUCKET_DIRECTORY = "content"
MAX_FILENAME_LENGTH = 240

s3 = resource("s3")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def save_content(url: str):
    local_domain = urlparse(url).netloc
    response = requests.get(url, allow_redirects=False)
    if url.endswith((".csv", ".xlsx", ".pdf", ".txt")):
        save_file(url, local_domain, response)
    else:
        save_raw_page(url, local_domain, response)


def run(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        for record in event["Records"]:
            try:
                save_content(record["body"])
            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record["messageId"]})
            sleep(randint(0, 1))

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
