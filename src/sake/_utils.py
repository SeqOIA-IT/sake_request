"""Define internal utils function."""

from __future__ import annotations

# std import
import typing

# 3rd party import
from tqdm.auto import tqdm

# project import

if typing.TYPE_CHECKING:
    # std import
    import collections

__all__ = ["wrap_iterator"]


def wrap_iterator(
    activate_tqdm: bool,  # noqa: FBT001
    iterator: collections.abc.Iterable[typing.Any],
    *,
    total: int | None = None,
) -> collections.abc.Iterable[typing.Any]:
    """Wrap iterator on tqdm or not."""
    if activate_tqdm:
        if total is None:
            return tqdm(iterator)
        return tqdm(iterator, total=total)
    return iterator
