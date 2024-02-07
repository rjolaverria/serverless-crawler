import logging
import random
import requests
from time import sleep
from urllib.parse import urlparse
from utils.files import save_file, save_raw_page
from utils.links import get_domain_hyperlinks, mark_and_enqueue_links


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def crawl(url: str):
    # Parse the URL and get the domain
    local_domain = urlparse(url).netloc

    response = requests.get(url, allow_redirects=False)

    if url.endswith((".csv", ".xlsx", ".pdf", ".txt")):
        save_file(url, local_domain, response)
    else:
        save_raw_page(url, local_domain, response)
        # Get the hyperlinks from the URL and add them to the queue
        links = get_domain_hyperlinks(local_domain, url, response)
        mark_and_enqueue_links(local_domain, links)


def run(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        for record in event["Records"]:
            try:
                crawl(record["body"])
            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record["messageId"]})
                logger.error("An error occurred while crawling")
                logger.error(e)

            #  Random delay between 0 and 0.8 seconds to avoid throttling or getting blocked
            delay = round(random.uniform(0, 0.8), 1)
            sleep(delay)

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
