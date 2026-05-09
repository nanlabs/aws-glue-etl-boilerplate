"""
Public API Raw ingestion job.

Extracts data from a configurable public API endpoint and stores JSONL records in S3 raw zone.
Authentication is optional and resolved via environment variables / Glue job arguments.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pydantic import Field

from libs.common import RawJobConfig, setup_logging
from libs.pyshell import PyShellJobBase

setup_logging()
logger = logging.getLogger(__name__)


class PublicApiRawConfig(RawJobConfig):
    source_name: str = Field(default="public_api")
    entity_type: str = Field(default="posts")
    api_base_url: str = Field(default="https://jsonplaceholder.typicode.com")
    api_endpoint: str = Field(default="/posts")
    max_retries: int = Field(default=3)
    batch_size: int = Field(default=500)
    rate_limit_per_second: float = Field(default=5.0)
    raw_write_path: Optional[str] = Field(default=None)

    def model_post_init(self, __context) -> None:
        self.raw_write_path = (
            f"{self.raw_zone_path.rstrip('/')}/{self.source_name}/{self.entity_type}/"
        )


class PublicApiRawJob(PyShellJobBase):
    def __init__(self, config: PublicApiRawConfig):
        super().__init__(config)

    def extract(self) -> Tuple[List[Dict], List[Dict]]:
        endpoint = self.config.api_endpoint
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        url = f"{self.config.api_base_url.rstrip('/')}{endpoint}"
        response_data, request_info = self.make_api_request(url=url)

        if isinstance(response_data, list):
            records = response_data
        elif isinstance(response_data, dict):
            records = response_data.get("data", [response_data])
        else:
            records = []

        return records, [request_info]

    def transform(
        self, data: Tuple[List[Dict], List[Dict]]
    ) -> Tuple[List[Dict], List[Dict]]:
        records, request_info = data
        now = datetime.utcnow()
        for record in records:
            record.setdefault("ingested_at", now.isoformat() + "Z")
            record.setdefault("source", self.config.source_name)
            record.setdefault("entity_type", self.config.entity_type)
        return records, request_info

    def load(self, data: Tuple[List[Dict], List[Dict]]) -> None:
        records, request_info = data
        self.write_to_s3(records, request_info_list=request_info)


if __name__ == "__main__":
    config = PublicApiRawConfig.from_args()
    logger.info("Starting public API raw extraction: %s", config.entity_type)
    PublicApiRawJob(config).run()
