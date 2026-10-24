from datetime import date
from typing import Any

from ccflow import ModelRegistry
from ccflow_etl import DatasetDefinition
from pydantic import BaseModel, Field

__all__ = ("DatasetOutputMixin",)


class DatasetOutputMixin(BaseModel):
    """Lets an extract model write to a date-partitioned dataset instead of its own output store and key layout.

    When ``dataset`` is set it supplies the store, the key, the name, and the first date. Models without it keep their
    ``output``, ``output_key``, and ``dataset_name``.
    """

    dataset: DatasetDefinition | None = Field(
        default=None,
        description="Date-partitioned dataset to write to. When set, it supplies the store, key, name, and first date in place of output, output_key_prefix, and dataset_name.",
    )

    @property
    def _store(self) -> Any:
        if self.dataset is None:
            return getattr(self, "output", None)
        return ModelRegistry.root()[self.dataset.store] if isinstance(self.dataset.store, str) else self.dataset.store

    @property
    def _name(self) -> str:
        return self.dataset.name if self.dataset is not None else getattr(self, "dataset_name", "")

    def _partition_date(self, context: Any) -> date:
        value = context.date
        return value if isinstance(value, date) else date.fromisoformat(str(value))

    def _key(self, context: Any) -> str:
        return self.dataset.key(self._partition_date(context)) if self.dataset is not None else self.output_key(context)

    def _before_first_date(self, context: Any) -> bool:
        return self.dataset is not None and self.dataset.first_date is not None and self._partition_date(context) < self.dataset.first_date
