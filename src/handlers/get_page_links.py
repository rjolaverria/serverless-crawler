import datetime
import logging
from collections import deque
from urllib.parse import urlparse
import os
import random
import requests
from time import sleep
from boto3 import client
from utils.links import get_domain_hyperlinks

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
HTTP_URL_PATTERN = r"^http[s]*://.+"

BASE_URL = os.environ.get("BASE_URL")
SQS_QUEUE_URL = os.environ.get("CRAWL_QUEUE_URL")

sqs = client("sqs")


def crawl(url: str):
    # Parse the URL and get the domain
    local_domain = urlparse(url).netloc

    # Create a queue to store the URLs to crawl
    queue = deque([url])

    # Create a set to store the URLs that have already been seen (no duplicates)
    seen = set([url])

    # While the queue is not empty, continue crawling
    while queue:
        # Get the next URL from the queue
        url = queue.pop()

        # Send the URL to the SQS queue for processing
        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=url)

        response = requests.get(url, allow_redirects=False)

        # Get the hyperlinks from the URL and add them to the queue
        for link in get_domain_hyperlinks(local_domain, url, response):
            if link not in seen:
                queue.append(link)
                seen.add(link)

        delay = round(random.uniform(0, 0.8), 1)
        sleep(delay)
    logger.info("Done. Found " + str(len(seen)) + " links")


def run(event, context):
    current_time = datetime.datetime.now().time()

    crawl(BASE_URL)

    name = context.function_name
    logger.info("Your cron function " + name + " ran at " + str(current_time))
