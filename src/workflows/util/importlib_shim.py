from __future__ import annotations

from importlib.metadata import entry_points


def entry_points_for_group(group: str):
    """In Python 3.12 a breaking change was introduced into importlib.metadata
    whereby the no-arg entrypoints() method no longer returns a dict-like object
    keyed by group, instead it returns an instance of EntryPoints.
    Args:
        group: The importlib group
    Returns:
        A set of entry points for the specified group.
    """
    entry_points_or_dict = entry_points()
    if getattr(entry_points_or_dict, "select", None):
        return entry_points_or_dict.select(group=group)  # type: ignore
    else:
        return entry_points_or_dict[group]
