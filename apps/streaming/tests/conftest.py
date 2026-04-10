"""Pytest configuration for streaming tests.

The streaming service uses bare imports (e.g. `from logger import Logger`)
because it is designed to run with `src/streaming` as the working directory
inside the Docker container. We replicate that here by inserting the path
before any streaming imports occur.
"""
import sys
from pathlib import Path

_STREAMING_SRC = Path(__file__).parent.parent / "src" / "streaming"
if str(_STREAMING_SRC) not in sys.path:
    sys.path.insert(0, str(_STREAMING_SRC))
