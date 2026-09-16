"""Source hashes must include local dependency patches as well as engine code."""
import hashlib
from pathlib import Path


def engine_source_hashes(root=Path(".")):
    root = Path(root)
    paths = set((root / "src").rglob("*.rs"))
    paths.update(root / name for name in ("Cargo.toml", "Cargo.lock"))
    paths.update(path for path in (root / "vendor").rglob("*") if path.is_file())
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }
