from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import Enum
from itertools import chain
from typing import MutableMapping  # TODO move to collections.abc (needs Python >=3.9)
from typing import TYPE_CHECKING, Any, TypeVar

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")

if TYPE_CHECKING:
    # TODO import from typing (needs Python >=3.11)
    from typing_extensions import Self


class NoDefault(Enum):
    nodefault = None


nodefault = NoDefault.nodefault


class ZictBase(MutableMapping[KT, VT]):
    """Base class for zict mappings"""

    def update(  # type: ignore[override]
        self,
        other: Mapping[KT, VT] | Iterable[tuple[KT, VT]] = (),
        /,
        **kwargs: VT,
    ) -> None:
        if hasattr(other, "items"):
            other = other.items()
        other = chain(other, kwargs.items())  # type: ignore
        self._do_update(other)

    def _do_update(self, items: Iterable[tuple[KT, VT]]) -> None:
        # Default implementation, can be overriden for speed
        for k, v in items:
            self[k] = v

    def close(self) -> None:
        """Release any system resources held by this object"""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


def close(*z: Any) -> None:
    """Close *z* if possible."""
    for zi in z:
        if hasattr(zi, "close"):
            zi.close()


def flush(*z: Any) -> None:
    """Flush *z* if possible."""
    for zi in z:
        if hasattr(zi, "flush"):
            zi.flush()
