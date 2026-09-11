import unittest

from benchmark.contract import ContractError
from benchmark.reference_runtime import LANCE_IO_QUOTA, apply_reference_runtime, reference_runtime


class ReferenceRuntimeTests(unittest.TestCase):
    def test_quota_is_reference_only_and_recorded(self):
        parent = {"UNRELATED": "preserved"}
        setup = dict(track="lance", **reference_runtime("lance", 16, parent))
        child = dict(parent)
        observed = apply_reference_runtime(setup, child)
        self.assertEqual(child[LANCE_IO_QUOTA], "16")
        self.assertNotIn(LANCE_IO_QUOTA, parent)
        self.assertFalse(observed["engine_controlled"])
        self.assertEqual(observed["lance_process_io_threads_limit"], "16")

    def test_default_does_not_set_quota(self):
        child = {}
        apply_reference_runtime({"track": "lance"}, child)
        self.assertNotIn(LANCE_IO_QUOTA, child)

    def test_invalid_or_unrecorded_controls_fail(self):
        for limit in (0, -1, True, "16"):
            with self.assertRaises(ContractError):
                reference_runtime("lance", limit, {})
        with self.assertRaises(ContractError):
            reference_runtime("raw_parquet", 16, {})
        for limit in (None, 16):
            with self.assertRaises(ContractError):
                apply_reference_runtime({"track": "lance", "reference_lance_io_limit": limit},
                                        {LANCE_IO_QUOTA: "32"})


if __name__ == "__main__":
    unittest.main()
