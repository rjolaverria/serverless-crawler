import logging
import os
import requests
from bs4 import BeautifulSoup
from boto3 import resource
from urllib.parse import urlparse

PAGES_BUCKET_NAME = os.environ.get("PAGES_BUCKET_NAME")
CONTENT_BUCKET_DIRECTORY = "content"
RAW_BUCKET_DIRECTORY = "raw"
MAX_FILENAME_LENGTH = 240

s3 = resource("s3")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def remove_newlines(text: str):
    # Replace newlines and extra spaces
    text = text.replace("\n", " ")
    text = text.replace("\\n", " ")
    text = text.replace("  ", " ")
    text = text.replace("  ", " ")
    return text


def get_file_path(
    url: str,
    local_domain: str,
    prefix: str = CONTENT_BUCKET_DIRECTORY,
    extension: str = None,
):
    try:
        file_path = (
            prefix
            + "/"
            + local_domain
            + "/"
            + url[8:].replace("/", "_")[:MAX_FILENAME_LENGTH]
        )
        if extension is not None:
            return file_path + extension
        else:
            # Get the file extension from the URL
            path = urlparse(url).path
            file_extension = os.path.splitext(path)[1]
            if file_extension is not None:
                return file_path + file_extension
            else:
                return file_path + ".html"
    except Exception as e:
        logger.error("An error occurred while generating the path:", e)
        raise Exception(e)


def save_text_from_page(url: str, local_domain: str, response: requests.Response):
    file_path = get_file_path(url, local_domain, extension=".txt")
    try:
        logger.info("Getting page content from: " + url)

        if response.status_code != 200:
            raise Exception("Response status code:", response.status_code)

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text()
        text = remove_newlines(text)
        object = s3.Object(PAGES_BUCKET_NAME, file_path)
        object.put(Body=text, Metadata={"sourceUrl": url})
    except requests.exceptions.RequestException as e:
        logger.error("Error retrieving page content:", e)
        raise Exception(e)
    except Exception as e:
        logger.error("An error occurred:", e)
        raise Exception(e)


def save_file(url: str, local_domain: str, response: requests.Response):
    file_path = get_file_path(url, local_domain, prefix=RAW_BUCKET_DIRECTORY)
    try:
        logger.info("Getting file from: " + url)
        if response.status_code != 200:
            raise Exception("Response status code:", response.status_code)

        object = s3.Object(PAGES_BUCKET_NAME, file_path)
        object.put(Body=response.content, Metadata={"sourceUrl": url})

    except requests.exceptions.RequestException as e:
        logger.error("Error retrieving file from URL:", e)
        raise Exception(e)
    except Exception as e:
        logger.error("An error occurred:", e)
        raise Exception(e)


def save_raw_page(url: str, local_domain: str, response: requests.Response):
    file_path = get_file_path(url, local_domain, prefix=RAW_BUCKET_DIRECTORY)
    try:
        logger.info("Getting page content from: " + url)

        if response.status_code != 200:
            raise Exception("Response status code:", response.status_code)

        object = s3.Object(PAGES_BUCKET_NAME, file_path)
        object.put(Body=response.text, Metadata={"sourceUrl": url})
    except requests.exceptions.RequestException as e:
        logger.error("Error retrieving page content:", e)
        raise Exception(e)
    except Exception as e:
        logger.error("An error occurred:", e)
        raise Exception(e)
