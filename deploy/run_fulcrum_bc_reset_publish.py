import argparse
import sys
from pathlib import Path
from pprint import pprint


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from app.fulcrum.bc_reset_publish import (  # noqa: E402
    build_cleanup_candidate_report,
    parse_reviewed_metafield_spec,
    reset_and_republish_bigcommerce_links,
    write_cleanup_candidate_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset old Fulcrum/v1 BigCommerce metafields and republish current approved outputs.")
    parser.add_argument("--store-hash", required=True)
    parser.add_argument("--execute", action="store_true", help="Actually delete remote metafields and republish approved outputs.")
    parser.add_argument("--product-id", action="append", type=int, default=[], help="Dry-run only: scan one product ID. Can be passed multiple times.")
    parser.add_argument("--category-id", action="append", type=int, default=[], help="Dry-run only: scan one category ID. Can be passed multiple times.")
    parser.add_argument("--max-entities", type=int, default=None, help="Dry-run only: stop after scanning this many products/categories.")
    parser.add_argument(
        "--delete-reviewed-metafield",
        action="append",
        default=[],
        help="Execute only a reviewed target using `<product|category>:<entity_id>:<metafield_id>`. Can be passed multiple times.",
    )
    parser.add_argument(
        "--export-cleanup-report",
        default="",
        help="Write read-only cleanup candidates to both .json and .csv files using this base path.",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for cleanup candidate review exports.")
    parser.add_argument(
        "--storefront-check-hints",
        action="store_true",
        help="Include PowerShell storefront HTML check commands in exported cleanup reports.",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.execute and args.export_cleanup_report:
        parser.error("--execute cannot be combined with --export-cleanup-report.")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    reviewed_metafields = [parse_reviewed_metafield_spec(value) for value in args.delete_reviewed_metafield]
    if args.export_cleanup_report:
        report = build_cleanup_candidate_report(
            args.store_hash,
            product_ids=args.product_id,
            category_ids=args.category_id,
            max_entities=args.max_entities,
            batch_size=args.batch_size,
            storefront_check_hints=args.storefront_check_hints,
        )
        output_paths = write_cleanup_candidate_report(report, args.export_cleanup_report)
        pprint(
            {
                "status": "ok",
                "report_only": True,
                "candidate_count": report["candidate_count"],
                "batch_count": report["batch_count"],
                "candidate_counts_by_reason": report["candidate_counts_by_reason"],
                "output_paths": output_paths,
            }
        )
        return

    pprint(
        reset_and_republish_bigcommerce_links(
            args.store_hash,
            execute=args.execute,
            product_ids=args.product_id,
            category_ids=args.category_id,
            max_entities=args.max_entities,
            reviewed_metafields=reviewed_metafields,
        )
    )


if __name__ == "__main__":
    main()
