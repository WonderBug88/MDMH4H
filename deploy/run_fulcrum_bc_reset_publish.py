import argparse
import sys
from pathlib import Path
from pprint import pprint


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from app.fulcrum.bc_reset_publish import reset_and_republish_bigcommerce_links  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset old Fulcrum/v1 BigCommerce metafields and republish current approved outputs.")
    parser.add_argument("--store-hash", required=True)
    parser.add_argument("--execute", action="store_true", help="Actually delete remote metafields and republish approved outputs.")
    parser.add_argument("--product-id", action="append", type=int, default=[], help="Dry-run only: scan one product ID. Can be passed multiple times.")
    parser.add_argument("--category-id", action="append", type=int, default=[], help="Dry-run only: scan one category ID. Can be passed multiple times.")
    parser.add_argument("--max-entities", type=int, default=None, help="Dry-run only: stop after scanning this many products/categories.")
    args = parser.parse_args()

    pprint(
        reset_and_republish_bigcommerce_links(
            args.store_hash,
            execute=args.execute,
            product_ids=args.product_id,
            category_ids=args.category_id,
            max_entities=args.max_entities,
        )
    )


if __name__ == "__main__":
    main()
