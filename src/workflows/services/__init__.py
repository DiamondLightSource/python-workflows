from __future__ import annotations

from importlib.metadata import entry_points


def lookup(service: str):
    """Find a service class based on a name.

    Args:
        service: Name of the service

    Returns:
        A service class
    """
    service_factory = get_known_services().get(service)
    if service_factory:
        return service_factory()
    else:
        return None


def get_known_services():
    """Return a dictionary of all known services.

    Returns:
        A dictionary containing entries { service name : service class factory }
        where a factory is a function that takes no arguments and returns an
        uninstantiated service class.
    """
    if not hasattr(get_known_services, "cache"):
        setattr(
            get_known_services,
            "cache",
            {e.name: e.load for e in entry_points(group="workflows.services")},
        )
    register = get_known_services.cache.copy()  # type: ignore
    return register
