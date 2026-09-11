"""Explicit reference-only runtime controls, applied before provider imports."""
from .contract import require

LANCE_IO_QUOTA = "LANCE_PROCESS_IO_THREADS_LIMIT"


def reference_runtime(track, limit, environment):
    require(limit is None or (type(limit) is int and limit > 0),
            "reference Lance I/O limit must be a positive integer")
    require(limit is None or track == "lance",
            "reference Lance I/O limit requires the Lance track")
    inherited = environment.get(LANCE_IO_QUOTA)
    if track == "lance":
        require(inherited is None,
                "unset inherited LANCE_PROCESS_IO_THREADS_LIMIT; use --reference-lance-io-limit to record a reference-only control")
    return {"reference_lance_io_limit": limit}


def apply_reference_runtime(setup, environment):
    limit = setup.get("reference_lance_io_limit")
    # Validate again in the independent worker, including direct invocations.
    reference_runtime(setup["track"], limit, environment)
    if limit is not None:
        environment[LANCE_IO_QUOTA] = str(limit)
    return {"lance_process_io_threads_limit": environment.get(LANCE_IO_QUOTA),
            "scope": "reference provider I/O concurrency; not a process thread cap",
            "engine_controlled": False}
