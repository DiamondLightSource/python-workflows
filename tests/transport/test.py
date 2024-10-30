from __future__ import annotations

import workflows.transport
from workflows.transport.common_transport import CommonTransport


def test_known_transports_is_a_dictionary():
    """Check transport register build in CommonTransport."""
    transports = workflows.transport.get_known_transports()
    print(transports)
    assert isinstance(transports, dict)


def test_enumerate_transports():
    """Verify we can discover the installed transports."""
    transports = workflows.transport.get_known_transports()
    assert transports.keys() == {"OfflineTransport", "PikaTransport", "StompTransport"}
    assert all(
        issubclass(transport, CommonTransport) for transport in transports.values()
    )


def test_load_any_transport():
    """Look up fallback transport mechanism."""
    assert workflows.transport.lookup(None)
