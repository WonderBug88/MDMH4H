from pathlib import Path

from app.fulcrum.config import Config
from app.fulcrum.services import apply_runtime_schema, upsert_store_installation


def _parse_credential_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip().upper()] = value.strip()
    return values


def _seed_manual_installation(store_hash: str, access_token: str, source: str) -> bool:
    if not store_hash or not access_token:
        return False

    upsert_store_installation(
        store_hash=store_hash,
        context=f"stores/{store_hash}",
        access_token=access_token,
        scope="manual_token_seed",
        install_source=source,
        metadata={"seeded": True},
    )
    return True


def main() -> None:
    apply_runtime_schema()

    seeded = 0
    if Config.BIG_COMMERCE_STORE_HASH and Config.BIG_COMMERCE_ACCESS_TOKEN:
        if _seed_manual_installation(
            store_hash=Config.BIG_COMMERCE_STORE_HASH,
            access_token=Config.BIG_COMMERCE_ACCESS_TOKEN,
            source="legacy_env",
        ):
            seeded += 1

    root_dir = Path(Config.FULCRUM_ENV_PATH).resolve().parent
    for path in list(root_dir.glob("BigCommerceAPI-credentials-*.txt")) + [root_dir / "Sandbox_credentials.txt"]:
        values = _parse_credential_file(path)
        api_path = values.get("API PATH", "")
        access_token = values.get("ACCESS TOKEN", "")
        store_hash = ""
        if "/stores/" in api_path:
            store_hash = api_path.split("/stores/", 1)[1].split("/", 1)[0]

        if _seed_manual_installation(
            store_hash=store_hash,
            access_token=access_token,
            source=f"credential_file:{path.name}",
        ):
            seeded += 1

    print(f"Fulcrum runtime ready. Seeded {seeded} installation record(s).")


if __name__ == "__main__":
    main()
