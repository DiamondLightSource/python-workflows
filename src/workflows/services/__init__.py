from __future__ import annotations

import pkg_resources


def lookup(service: str):
    """Find a service class based on a name.
    :param service: Name of the service
    :return: A service class
    """
    service_factory = get_known_services().get(service)
    if service_factory:
        return service_factory()
    else:
        return None


def get_known_services():
    """Return a dictionary of all known services.
    :return: A dictionary containing entries { service name : service class factory }
             A factory is a function that takes no arguments and returns an
             uninstantiated service class.
    """
    if not hasattr(get_known_services, "cache"):
        setattr(
            get_known_services,
            "cache",
            {
                e.name: e.load
                for e in pkg_resources.iter_entry_points("workflows.services")
            },
        )
    register = get_known_services.cache.copy()
    return register
