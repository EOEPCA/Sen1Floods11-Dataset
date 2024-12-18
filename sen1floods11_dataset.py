"""
This module will be used in load_dataset method from Datasets library from 
HuggingFace. The goal is to load the dataset Sen1floods11Dataset and stream the
data.
"""

import os
from io import BytesIO

import datasets.filesystems
import datasets
import numpy as np
import rasterio
from dvc.api import DVCFileSystem
from datasets import BuilderConfig

S1 = "v1.1/data/flood_events/HandLabeled/S1Hand/"
LABELS = "v1.1/data/flood_events/HandLabeled/LabelHand/"


class CustomBuilderConfig(BuilderConfig):
    """
    This class is used to transfer configurations variables (such as 'no_cache'
    option and 'context' path) to the class Sen1floods11Dataset in self config
    field.
    """

    def __init__(self, version="1.0.0", description=None, **kwargs):
        super().__init__(version=version, description=description)
        config = kwargs.get("config_kwargs")
        self.no_cache = config["no_cache"]
        self.context = config["context"]


class Sen1floods11Dataset(datasets.GeneratorBasedBuilder):
    """
    A custom dataset class for loading and preprocessing the Sen1Floods11
    dataset, designed for flood detection using Sentinel-1 satellite imagery.

    This class uses the Hugging Face `datasets` library to handle data
    generation and implements specific logic to load, process, and stream the
    dataset efficiently.

    Main Features:
        - Custom Configuration: Uses a `CustomBuilderConfig` to handle optional
    parameters like `no_cache`, which enables on-the-fly reading of data without
    saving it to the local cache.
        - File System Integration: Integrates with a DVCFileSystem to fetch data
    dynamically (e.g., from DVC remote storage).
        - Data Preprocessing: Applies transformations to both
    images and masks:
        - Images are clipped, normalized, and converted to a range [0, 1].
        - Streaming capability: Supports streaming mode, which allows the
        dataset to be loaded progressively, ideal for working with large
    datasets that cannot fit into memory.

    Dataset Split Information: - Train: Training split with associated images
    and masks. - Validation: Validation split for hyperparameter tuning. - Test:
    Test split for model evaluation.

    Output Data: - Each example consists of:
        - `image`: A preprocessed 3D array (512x512x2) representing the
            Sentinel-1 imagery.
        - `mask`: A preprocessed 3D array (512x512x1) representing the ground
            truth flood masks.

    Usage: - The class is compatible with the Hugging Face datasets library and
    can be used for tasks like model training, validation, and testing in flood
    detection pipelines.
    """

    BUILDER_CONFIG_CLASS = CustomBuilderConfig

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fs = DVCFileSystem(self.config.context)

    def _info(self):
        """Contains informations and typings for the dataset."""

        return datasets.DatasetInfo(
            description="Sen1Floods11 - Dataset for flood detection using Sentinel-1 data.",
            features=datasets.Features(
                {
                    "image": datasets.Array3D(shape=(512, 512, 2), dtype="float32"),
                    "mask": datasets.Array3D(shape=(512, 512, 1), dtype="int32"),
                }
            ),
        )

    def _split_generators(self, dl_manager):
        data_dir = "sen1floods11-dataset"
        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={"csv_file": f"{data_dir}/flood_train_data.csv"},
            ),
            datasets.SplitGenerator(
                name=datasets.Split.VALIDATION,
                gen_kwargs={"csv_file": f"{data_dir}/flood_valid_data.csv"},
            ),
            datasets.SplitGenerator(
                name=datasets.Split.TEST,
                gen_kwargs={"csv_file": f"{data_dir}/flood_test_data.csv"},
            ),
        ]

    def load_input(self, input_path, image_name, no_cache):
        """
        If --no-cache option is enabled, just read input on the fly (input is
        downloaded but not saved). If --no-cache option is disabled and theinput
        is in cache, load the input from the cache. Otherwise Download the input
        and save it in cache.
        """
        local_path = os.path.join(self.config.context, input_path, image_name)
        dvc_path = "/" + input_path + image_name
        if no_cache:
            binary_image = self.fs.read_bytes(dvc_path)
            image_stream = BytesIO(binary_image)
            with rasterio.open(image_stream) as src:
                image = src.read()
        elif not os.path.isfile(local_path) and not no_cache:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            binary_image = self.fs.read_bytes(dvc_path)
            image_stream = BytesIO(binary_image)

            with rasterio.open(image_stream) as src:
                profile = src.profile
                image = src.read()
                with rasterio.open(local_path, "w", **profile) as dst:
                    dst.write(image)
        else:
            with rasterio.open(local_path) as src:
                image = src.read()
        return image

    def process_image(self, image):
        """Process the image that will be the model input."""
        image = np.nan_to_num(image)
        image = np.clip(image, -50, 1)
        image = (image + 50) / 51
        return image

    def process_mask(self, mask):
        """Process the mask that will be compared to the model output."""
        mask[mask == -1] = 255
        return mask

    def _generate_examples(self, **kwargs):
        """When iterating the dataset, yield image & mask."""
        csv_file = kwargs.get("csv_file")
        no_cache = self.config.no_cache
        with open(csv_file, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                image_name, mask_name = line.strip().split(",")

                image = self.load_input(S1, image_name, no_cache)
                image = self.process_image(image)

                mask = self.load_input(LABELS, mask_name, no_cache)
                mask = self.process_mask(mask)

                yield idx, {
                    "image": image,
                    "mask": mask,
                }
