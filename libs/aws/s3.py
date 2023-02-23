from typing import Union
import boto3
from libs.config import get_config


class AwsS3Client:
    session = None
    s3 = None
    bucket_name = None

    def __init__(
        self,
        bucket_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
        aws_session_token: str = None,
        endpoint_url: str = None,
    ):
        """
        :param bucket_name: The name of the bucket.
        :param aws_access_key_id: The access key for your AWS account.
        :param aws_secret_access_key: The secret key for your AWS account.
        :param region_name: The AWS region to connect to.
        :param aws_session_token: The session token for your AWS account.
        :param endpoint_url: The URL to use when connecting to S3.
        """
        config = get_config()

        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id or config.aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key or config.aws_secret_access_key,
            region_name=region_name or config.aws_region,
            aws_session_token=aws_session_token or config.aws_session_token,
        )
        self.s3 = self.session.resource(
            "s3", endpoint_url=endpoint_url or config.aws_endpoint_url
        )
        self.bucket_name = (
            bucket_name if bucket_name is not None else config.datalake_bucket_name
        )

    def get_object(self, key: str) -> Union[str, None]:
        """
        Get an object from the S3 bucket.
        If the object does not exist, returns None.

        :param key: The name of the object.
        :return: The object's value, or None if the object doesn't exist.
        """
        bucket = self.s3.Bucket(self.bucket_name)
        object_exists = False

        for _i in bucket.objects.filter(Prefix=key):
            object_exists = True

        if object_exists:
            obj = self.s3.Object(self.bucket_name, key)
            return obj.get()["Body"].read()

        return None

    def put_object(self, data: bytes, key: str) -> dict:
        """
        Adds an object to a bucket. You must have WRITE permissions on a bucket
        to add an object to it.
        Amazon S3 never adds partial objects; if you receive a success response,
        Amazon S3 added the entire object to the bucket.

        :param data: The data of the object encoded into bytes.
        :param key: The URI and the object's name.
        :return: Dict with S3's answer upon inserting the object.
        """
        obj = self.s3.Object(self.bucket_name, key)

        return obj.put(Body=data)

    def get_objects(self, key: str = None, delimiter: str = None) -> list:
        """
        Returns all items in a specific folder.

        :param key: (Optional) The URI and the object's name.
        :return: Console prints the list of object in a given URI.
        """
        bucket = self.s3.Bucket(self.bucket_name)

        return list(bucket.objects.filter(Prefix=key, Delimiter=delimiter))

    def get_all_keys(self, delimiter: str = None) -> list:
        """
        Returns all keys in the bucket.

        :param delimiter: (Optional) The delimiter is a character you use to group keys.
        :return: A list of all keys in the bucket.
        """

        paginator = self.get_paginator()
        pages = paginator.paginate(Bucket=self.bucket_name, Delimiter=delimiter)

        return [
            page.get("Prefix")
            for page in pages.search("CommonPrefixes")
            if page is not None
        ]

    def get_paginator(self):
        return self.s3.meta.client.get_paginator("list_objects")
