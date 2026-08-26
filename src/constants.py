from pathlib import Path


GLOBAL_UPLOAD_PATH = Path("public/uploads")


def get_upload_path(filename: str | Path) -> Path:
    upload_root = (Path.cwd() / GLOBAL_UPLOAD_PATH).resolve()
    requested_path = Path(filename)

    if requested_path.is_absolute():
        resolved_path = requested_path.resolve()

        if (
            resolved_path == upload_root
            or upload_root in resolved_path.parents
        ):
            return resolved_path

        requested_path = Path(requested_path.name)
    else:
        upload_parts = GLOBAL_UPLOAD_PATH.parts

        if requested_path.parts[:len(upload_parts)] == upload_parts:
            requested_path = Path(
                *requested_path.parts[len(upload_parts):]
            )

    resolved_path = (upload_root / requested_path).resolve()

    if (
        resolved_path != upload_root
        and upload_root not in resolved_path.parents
    ):
        raise ValueError("upload path must be inside public/uploads")

    return resolved_path
