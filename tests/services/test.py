from __future__ import annotations

import workflows.services


def test_known_services_is_a_dictionary():
    """Check services register build in CommonService."""
    assert isinstance(workflows.services.get_known_services(), dict)
