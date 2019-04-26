from __future__ import absolute_import, division, print_function

import pkg_resources


def lookup(service):
    """Find a service class based on a name.
    :param service: Name of the service
    :return: A service class
    """
    return get_known_services().get(service)


def get_known_services():
    """Return a dictionary of all known services.
    :return: A dictionary containing entries { service name : service class }

    Future: This will change to a dictionary containing references to
            factories:               { service name : service class factory }
            A factory is a function that takes no arguments and returns an
            uninstantiated service class. This will avoid importing all
            service classes.
    """
    if not hasattr(get_known_services, "cache"):
        setattr(
            get_known_services,
            "cache",
            {
                e.name: e.load()
                for e in pkg_resources.iter_entry_points("workflows.services")
            },
        )
    register = get_known_services.cache.copy()
    return register
