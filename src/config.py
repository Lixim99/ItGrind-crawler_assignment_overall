import json
from pathlib import Path

from .storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    PostgreSQLStorage,
)


def load_config(
    filename: str,
) -> dict:
    path = Path(filename)

    if not path.exists():
        raise FileNotFoundError(
            f"Config not found: {filename}"
        )

    with path.open(
        mode="r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def create_storage(
    config: dict,
) -> DataStorage:
    storage_type = config.get(
        "type"
    )

    if storage_type == "json":
        options = {}

        if "formatted" in config:
            options["formatted"] = config["formatted"]

        if "indent" in config:
            options["indent"] = config["indent"]

        return JSONStorage(
            config["filename"],
            **options,
        )

    if storage_type == "csv":
        return CSVStorage(
            config["filename"],
            encoding=config.get(
                "encoding",
                "utf-8",
            ),
        )

    if storage_type == "postgresql":
        return PostgreSQLStorage(
            database=config.get(
                "database",
                "crawler",
            )
        )

    raise ValueError(
        f"Unknown storage type: {storage_type}"
    )
