import argparse
import logging
import time

from data_lifecycle.azcopy_runner import (
    AzCopyJobNotFoundError,
    get_summary,
    poll,
    start_copy,
)
from data_lifecycle.storage_resolver import (
    resolve_backend_client,
    resolve_location,
)
from data_lifecycle.storage_types import StorageRole

POLL_MAX_RETRIES = 3

logger = logging.getLogger("scripts.cool_data")
logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ps",
        "--project-slug",  # pylint: disable=duplicate-code
        help="Slug of the project of the data you want to cool down",
        required=True,
    )

    args = parser.parse_args()
    hot_location = resolve_location(StorageRole.HOT, args.project_slug)
    cool_location = resolve_location(StorageRole.COOL, args.project_slug)

    hot_client = resolve_backend_client(StorageRole.HOT)
    cool_client = resolve_backend_client(StorageRole.COOL)

    source_url = (
        hot_location.uri
        + "/*?"
        + hot_client.generate_project_directory_token(
            project_name=args.project_slug,
            permission={
                "list": True,
                "read": True,
                "add": False,
                "create": False,
                "write": False,
                "delete": False,
            },
        )
    )

    dest_url = (
        cool_location.uri
        + "?"
        + cool_client.generate_project_directory_token(
            project_name=args.project_slug,
            permission={
                "read": True,
                "add": True,
                "create": True,
                "write": True,
                "delete": True,
            },
        )
    )

    job = start_copy(source_uri=source_url, dest_uri=dest_url)

    logger.info("Started job: %s", job)

    time.sleep(5)

    job_id_not_found_retries = 0

    while True:
        try:
            status = poll(job.job_id)
        except AzCopyJobNotFoundError as e:
            if job_id_not_found_retries >= POLL_MAX_RETRIES:
                raise e
            logger.warning("Job not found during polling. Retrying...")
            job_id_not_found_retries += 1
            time.sleep(5)
            continue

        logger.info("Job status: %s", status.state)
        if status.state in {"SUCCEEDED", "FAILED", "CANCELED"}:
            break
        time.sleep(5)

    summary = get_summary(job.job_id)

    logger.info("Job summary:")
    logger.info(summary)


if __name__ == "__main__":
    main()
