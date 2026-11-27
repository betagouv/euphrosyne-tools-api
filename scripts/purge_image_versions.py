"""
Delete the old versions of the VM image definitions of the Azure gallery.

Each gallery version is a billed snapshot. For each image definition, only the
most recent versions are kept (3 by default). A version is never deleted if it is
among the most recent ones or if an existing VM was created from it.

Nothing is deleted unless --no-dry-run is passed.
"""

import argparse

from clients.azure import VMAzureClient
from clients.azure.vm import DEFAULT_IMAGE_VERSIONS_TO_KEEP

from . import get_logger

logger = get_logger(__name__)


def purge_image_versions(
    azure_client: VMAzureClient,
    image_definitions: list[str] | None = None,
    keep: int = DEFAULT_IMAGE_VERSIONS_TO_KEEP,
    dry_run: bool = True,
) -> int:
    """Purges the old versions of the given image definitions (all of them if None).
    Returns the number of versions deleted (or that would be in dry-run mode)."""
    if image_definitions is None:
        image_definitions = azure_client.list_all_vm_image_definitions()

    versions_in_use = azure_client.get_image_versions_in_use()
    total = 0
    for image_definition in image_definitions:
        plan = azure_client.plan_image_versions_purge(
            image_definition, keep=keep, versions_in_use=versions_in_use
        )
        logger.info(
            "%s: keeping %s", image_definition, ", ".join(plan.kept) or "nothing"
        )
        if plan.in_use:
            logger.info(
                "%s: old versions kept because used by a VM: %s",
                image_definition,
                ", ".join(plan.in_use),
            )
        if plan.ignored:
            logger.warning(
                "%s: versions ignored (not major.minor.patch): %s",
                image_definition,
                ", ".join(plan.ignored),
            )
        if not plan.to_delete:
            logger.info("%s: no version to delete.", image_definition)
            continue

        total += len(plan.to_delete)
        if dry_run:
            logger.info(
                "%s: [dry-run] would delete %s",
                image_definition,
                ", ".join(plan.to_delete),
            )
            continue

        for version, poller in azure_client.purge_image_versions(plan):
            logger.info("Deleting %s %s...", image_definition, version)
            poller.result()
            logger.info("Deleted %s %s.", image_definition, version)
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-i",
        "--image",
        action="append",
        help="Image definition to purge. Can be repeated. Default: all of them.",
    )
    parser.add_argument(
        "-k",
        "--keep",
        type=int,
        default=DEFAULT_IMAGE_VERSIONS_TO_KEEP,
        help="Number of most recent versions to keep per image definition "
        f"(default: {DEFAULT_IMAGE_VERSIONS_TO_KEEP}).",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only list the versions to delete (default). "
        "Use --no-dry-run to actually delete them.",
    )
    args = parser.parse_args()
    if args.keep < 1:
        parser.error("--keep must be at least 1")

    total = purge_image_versions(
        VMAzureClient(),
        image_definitions=args.image,
        keep=args.keep,
        dry_run=args.dry_run,
    )
    logger.info(
        "%s version(s) %s.", total, "would be deleted" if args.dry_run else "deleted"
    )


if __name__ == "__main__":
    main()
