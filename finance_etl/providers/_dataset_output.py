from datetime import date

from ccflow_etl import DatasetDefinition

__all__ = ("before_first_date",)


def before_first_date(dataset: DatasetDefinition, value: date) -> bool:
    """Whether ``value`` precedes the dataset's first partition, so an extract for it has nothing to write."""
    return dataset.first_date is not None and value < dataset.first_date
