"""
This module declares a class generator that can be used with the `datasets`
library from HuggingFace.
"""

import csv
import random
from collections.abc import Callable, Generator, Iterator, Mapping
from io import BytesIO
from pathlib import Path
from types import EllipsisType
from typing import Any, Protocol, runtime_checkable

import numpy as np
import rasterio
from dvc.api import DVCFileSystem


@runtime_checkable
class DatasetGenerator(Protocol):
    def __len__(self) -> int: ...
    def __call__(self, *args: Any, **kwargs: Any) -> Generator[dict, None, None]: ...


class DatasetLoader(Mapping):
    SPLITS = {
        "train": "train_data.csv",
        "validation": "valid_data.csv",
        "test": "test_data.csv",
        "sample": "sample_data.csv",
    }

    def __init__(self, context: str | EllipsisType = ..., /) -> None:
        self.context = (
            Path(__file__).resolve().parent
            if context is Ellipsis
            else Path(context).resolve()
        )

    def __len__(self) -> int:
        return len(self.SPLITS)

    def __iter__(self) -> Iterator[str]:
        return iter(self.SPLITS)

    def __getitem__(self, key: str) -> DatasetGenerator:
        return FloodDatasetGenerator(
            context=self.context, split=self.SPLITS.get(key, key)
        )


class FloodDatasetGenerator(DatasetGenerator):
    S1 = "v1.1/data/flood_events/HandLabeled/S1Hand/"
    LABELS = "v1.1/data/flood_events/HandLabeled/LabelHand/"

    def __init__(
        self,
        context: Path,
        split: str,
    ) -> None:
        self.context = context
        self.split = split
        self._dvc_fs = DVCFileSystem(repo=self.context)

    def __len__(self) -> int:
        csv_file_path = self.context / self.split
        with csv_file_path.open() as f:
            csv_reader = csv.reader(f, delimiter=",")
            return len(tuple(csv_reader))

    def __call__(
        self,
        shuffle: bool = False,
        stream: bool = False,
        stream_cache: bool = True,
        process_func: Callable[[dict], dict] | None = None,
        **kwargs,
    ) -> Generator[dict, None, None]:
        csv_file_path = self.context / self.split
        with csv_file_path.open() as f:
            csv_reader = csv.reader(f, delimiter=",")
            rows = [*csv_reader]

            if shuffle:
                random.shuffle(rows)

            for row in rows:
                data = {
                    "image": self._load_image(
                        self.S1, row[0], stream=stream, stream_cache=stream_cache
                    ),
                    "mask": self._load_image(
                        self.LABELS, row[1], stream=stream, stream_cache=stream_cache
                    ),
                }
                if process_func:
                    data = process_func(data)
                yield data

    def _load_image(
        self,
        image_dir: str,
        image_name: str,
        stream: bool = False,
        stream_cache: bool = True,
    ) -> np.ndarray:
        local_path = self.context / image_dir / image_name
        if local_path.is_file():
            with rasterio.open(local_path) as src:
                image = src.read()
        elif stream:
            dvc_path = Path("/", image_dir, image_name)
            binary_image = self._dvc_fs.read_bytes(dvc_path)
            image_stream = BytesIO(binary_image)
            with rasterio.open(image_stream) as src:
                image = src.read()
                if stream_cache:
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    local_path.write_bytes(data=image_stream.getbuffer())
        else:
            raise RuntimeError(f"File not accessible: {local_path}")
        return image
