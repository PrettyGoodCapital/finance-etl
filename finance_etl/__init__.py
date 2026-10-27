__version__ = "0.1.0"

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .calendars import *
    from .providers.fred import *
    from .providers.massive import *
    from .providers.openfigi import *
    from .providers.sec import *
    from .symbols import *
    from .types import *

# Exports resolve on first access, importing only the submodule that defines them, so locating
# finance_etl.config for Hydra and using a single model do not import every provider and dependency.
_SUBMODULES = ("calendars", "providers.fred", "providers.massive", "providers.openfigi", "providers.sec", "symbols", "types")
_EXPORTS: dict[str, str] | None = None


def _exports() -> dict[str, str]:
    global _EXPORTS
    if _EXPORTS is None:
        from ast import Assign, Name, literal_eval, parse
        from pathlib import Path

        root = Path(__file__).parent
        _EXPORTS = {}
        for submodule in _SUBMODULES:
            source = root.joinpath(*submodule.split(".")).with_suffix(".py")
            for node in parse(source.read_text()).body:
                if isinstance(node, Assign) and any(isinstance(target, Name) and target.id == "__all__" for target in node.targets):
                    _EXPORTS.update(dict.fromkeys(literal_eval(node.value), submodule))
    return _EXPORTS


def __getattr__(name: str) -> object:
    if name == "__all__":
        return tuple(_exports())
    submodule = _exports().get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(f"{__name__}.{submodule}"), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_exports()})
