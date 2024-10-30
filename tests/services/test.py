from __future__ import annotations

import workflows.services
from workflows.services.common_service import CommonService


def test_known_services_is_a_dictionary():
    """Check services register build in CommonService."""
    assert isinstance(workflows.services.get_known_services(), dict)


def test_enumerate_services():
    """Verify we can discover the installed services."""
    services = workflows.services.get_known_services()
    assert services.keys() == {
        "SampleConsumer",
        "SampleProducer",
        "SampleTxn",
        "SampleTxnProducer",
    }
    assert all(issubclass(service(), CommonService) for service in services.values())
