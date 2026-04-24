"""Custom dataset that wraps PartitionedDataset with graceful empty-directory handling."""

from kedro.io import DatasetError
from kedro_datasets.partitions import PartitionedDataset


class NullablePartitionedDataset(PartitionedDataset):
    """PartitionedDataset that returns an empty dict instead of raising when no partitions exist.

    Kedro's PartitionedDataset raises DatasetError on load when the path is
    missing or empty. This subclass catches that error so pipelines can treat
    a missing dataset as an empty collection — useful for first-run incremental
    ingestion where no historical data exists yet.
    """

    def load(self) -> dict:
        """Load partitions, returning {} if the path is missing or empty."""
        try:
            return super().load()
        except (DatasetError, FileNotFoundError):
            return {}
