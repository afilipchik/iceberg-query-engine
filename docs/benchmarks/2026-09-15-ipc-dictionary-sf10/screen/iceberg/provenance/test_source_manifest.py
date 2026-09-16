import tempfile
import unittest
from pathlib import Path

from .source_manifest import engine_source_hashes


class SourceManifestTests(unittest.TestCase):
    def test_local_dependency_changes_cannot_hide_behind_the_same_lockfile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("Cargo.toml", "Cargo.lock", "src/lib.rs",
                         "vendor/lance/src/read.rs", "vendor/lance/protos/read.proto"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(name)
            before = engine_source_hashes(root)
            changed = root / "vendor/lance/src/read.rs"
            changed.write_text("changed refinement behavior")
            after = engine_source_hashes(root)
            self.assertEqual(before["Cargo.lock"], after["Cargo.lock"])
            self.assertNotEqual(before["vendor/lance/src/read.rs"],
                                after["vendor/lance/src/read.rs"])
            self.assertIn("vendor/lance/protos/read.proto", after)
            changed.unlink()
            self.assertNotIn("vendor/lance/src/read.rs", engine_source_hashes(root))
