from __future__ import annotations

import workflows


def test_workflows_version_returns_sensible_value():
    """The version() function should return something
    resembling a version number."""
    assert workflows.version().index(".")
