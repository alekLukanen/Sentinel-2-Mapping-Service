from unittest import TestCase
import tempfile
import os
import json
import shutil
from PIL import Image
import numpy as np

from build_ndvi_map import build_ndvi_maps_for_boundaries
import build_ndvi_map


class TestBuildNDVIMap(TestCase):

    def setUp(self):
        self.boundary1_geojson = {
            "coordinates": [
                [
                    [
                        -98.29377108430018,
                        45.51545082233693
                    ],
                    [
                        -98.23596192672191,
                        45.513762969793305
                    ],
                    [
                        -98.23475756927279,
                        45.55341412123062
                    ],
                    [
                        -98.279318794906,
                        45.55004064352917
                    ],
                    [
                        -98.29377108430018,
                        45.51545082233693
                    ]
                ]
            ],
            "type": "Polygon"
        }

        base_path = os.path.dirname(os.path.dirname(build_ndvi_map.__file__))
        module_path = os.path.dirname(build_ndvi_map.__file__)
        self.source_band04_path = os.path.join(
            base_path, "worker/example_data/S2A_14TNR_20220716_0_L2A/B04.tif")
        self.source_band08_path = os.path.join(
            base_path, "worker/example_data/S2A_14TNR_20220716_0_L2A/B08.tif")

        # ensure the files are where they should be
        self.assertTrue(os.path.isfile(self.source_band04_path))
        self.assertTrue(os.path.isfile(self.source_band08_path))

    def test_build_ndvi_map_for_boundary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            band04_path = os.path.join(tmpdir, "satData_band04.tif")
            band08_path = os.path.join(tmpdir, "satData_band08.tif")
            boundary_file_path = os.path.join(
                tmpdir, "boundary_geometry_A17.json")

            # write the boundary to the directory
            with open(boundary_file_path, "wb") as boundary_file:
                boundary_file.write(json.dumps(
                    self.boundary1_geojson).encode("utf-8"))

            # copy over the tile locations
            shutil.copy(self.source_band04_path, band04_path)
            shutil.copy(self.source_band08_path, band08_path)

            build_ndvi_maps_for_boundaries(
                data_dir=tmpdir,
                band_prefix="satData_band",
                boundary_prefix="boundary_geometry_",
            )

            expected_png_path = os.path.join(tmpdir, "raster_image_A17.png")
            expected_meta_path = os.path.join(tmpdir, "raster_meta_A17.json")
            self.assertTrue(os.path.isfile(expected_png_path))
            self.assertTrue(os.path.isfile(expected_meta_path))

            with open(expected_meta_path, "rb") as meta_file:
                result_meta_data = json.loads(meta_file.read())

            self.assertEqual(
                {
                    'imageBounds': [[45.51366193452552, -98.29379230898496], [45.553832217548006, -98.23475752841048]],
                    'rasterMax': 0.92525187,
                    'rasterMean': 0.53262654,
                    'rasterMedian': 0.64665988,
                    'rasterMin': -1.0
                },
                result_meta_data,
            )

            # validate the image is correct
            with Image.open(expected_png_path) as result_image:
                result_image_data = np.asarray(result_image, dtype="int32")

            image_mean_value = np.mean(result_image_data)
            self.assertAlmostEqual(
                125.86744615336684, image_mean_value, places=6)
