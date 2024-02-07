import logging
import os
import re
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from utils.dynamo import CrawlTable
from boto3 import client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

HTTP_URL_PATTERN = r"^http[s]*://.+"
SQS_QUEUE_URL = os.environ.get("CRAWL_QUEUE_URL")

sqs = client("sqs")


def mark_as_seen(domain: str, link: str) -> bool:
    crawlTable = CrawlTable()
    try:
        item = crawlTable.get_link(
            domain=domain,
            link=link,
        )
        if item:
            logger.info("Link was already seen: " + link)
        else:
            crawlTable.put_link(domain=domain, link=link)
            logger.info("Link marked as seen: " + link)
            return True
    except Exception as e:
        logger.error("An error occurred while marking link as seen")
        logger.error(e)
    return False


def mark_and_enqueue_links(domain: str, links: list[str]):
    for link in links:
        try:
            if mark_as_seen(domain, link):
                sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=link)
        except Exception as e:
            raise Exception(e)


def get_hyperlinks(url: str, response: requests.Response) -> list[str]:
    logger.info("Getting hyperlinks from: " + url)
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        linksTags = soup.find_all("a")
        return [link.get("href") for link in linksTags]
    except requests.exceptions.RequestException as e:
        logger.error("An error occurred while making the request")
        return []
    except Exception as e:
        logger.error("An error occurred")
        return []


# Function to get the hyperlinks from a URL that are within the same domain
def get_domain_hyperlinks(
    local_domain: str, url: str, response: requests.Response
) -> list[str]:
    clean_links: list[str] = []
    for link in set(get_hyperlinks(url, response)):
        if not isinstance(link, str):
            continue

        clean_link = None

        # If the link is a URL, check if it is within the same domain
        if re.search(HTTP_URL_PATTERN, link):
            # Parse the URL and check if the domain is the same
            url_obj = urlparse(link)
            if url_obj.netloc == local_domain:
                clean_link = link

        # If the link is not a URL, check if it is a relative link
        else:
            if link.startswith("/"):
                link = link[1:]
            elif link.startswith("#") or link.startswith("mailto:"):
                continue
            clean_link = "https://" + local_domain + "/" + link

        if clean_link is not None:
            if clean_link.endswith("/"):
                clean_link = clean_link[:-1]
            clean_links.append(clean_link)

    # Return the list of hyperlinks that are within the same domain
    return list(set(clean_links))
