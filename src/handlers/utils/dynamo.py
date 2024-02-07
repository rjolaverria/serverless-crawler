from datetime import datetime
import logging
from boto3 import resource
import os
from botocore.exceptions import ClientError

CRAWL_TABLE_NAME = os.environ.get("CRAWL_TABLE_NAME")


class CrawlTable:
    """Encapsulates an Amazon DynamoDB CrawlTable data."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        try:
            self.table = resource("dynamodb").Table(CRAWL_TABLE_NAME)
        except Exception as e:
            print(f"Something went wrong getting the table: {e}")

    def put_link(self, domain: str, link: str):
        try:
            now = datetime.utcnow().isoformat()
            self.table.put_item(
                Item={
                    "PK": domain,
                    "SK": link,
                    "createdAt": now,
                    "updatedAt": now,
                },
            )

        except ClientError as err:
            self.logger.error(
                "Couldn't add link %s to table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_link(self, domain: str, link: str) -> dict:
        try:
            response = self.table.get_item(Key={"PK": domain, "SK": link})
            return response.get("Item")
        except ClientError as err:
            self.logger.error(
                "Couldn't get link %s from table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def update_link(self, domain: str, link: str):
        try:
            now = datetime.utcnow().isoformat()
            self.table.update_item(
                Key={"PK": domain, "SK": link},
                UpdateExpression="SET updatedAt = :updatedAt",
                ExpressionAttributeValues={
                    ":updatedAt": now,
                },
            )
        except ClientError as err:
            self.logger.error(
                "Couldn't update link %s from table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
