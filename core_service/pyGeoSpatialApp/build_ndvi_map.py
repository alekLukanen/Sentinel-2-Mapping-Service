
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
from shapely.geometry import shape
from shapely.ops import transform
from PIL import Image
from matplotlib import cm, colors, colormaps
import pyproj

import os
import json
import argparse
import tempfile


def find_boundary_file_names(data_dir: str, boundary_prefix: str):
    return  [
        el 
        for el in os.listdir(data_dir) 
        if el.startswith(boundary_prefix)
    ]


def parse_boundary_id(boundary_file_name: str, boundary_prefix: str):
    if not boundary_file_name.startswith(boundary_prefix) or not boundary_file_name.endswith(".json"):
        raise Exception("invalid boundary name")

    boundary_id = boundary_file_name.replace(boundary_prefix, "").replace(".json", "")
    if "." in boundary_id:
        raise Exception("invalid boundary id")

    return boundary_id



def build_boundary_shape(boundary_path, utm_projection):
    with open(boundary_path, "rb") as data:
        boundary_data = json.loads(data.read())
    
    boundary_shape = shape(boundary_data)
    wgs84 = pyproj.CRS('EPSG:4326')

    project = pyproj.Transformer.from_crs(
        wgs84, utm_projection, always_xy=True
    ).transform
    boundary_shape_utm = transform(project, boundary_shape)
    return boundary_shape_utm


def build_ndvi_maps_for_boundaries(data_dir: str, band_prefix: str, boundary_prefix: str):
    """
    create ndvi maps for the boundaries in the data directory with the given boundary prefix.
    """
    if not os.path.isdir(data_dir):
        raise Exception(f"not a valid data directory: {data_dir}")

    band04_path = os.path.join(data_dir, f"{band_prefix}04.tif")
    band08_path = os.path.join(data_dir, f"{band_prefix}08.tif")
    bandSCL_path = os.path.join(data_dir, f"{band_prefix}SCL.tif")
    if not os.path.isfile(band04_path) or not os.path.isfile(band08_path):
        raise Exception(f"missing satellite banded data")

    bandSCL_10m_path = None
    if os.path.isfile(bandSCL_path):
        bandSCL_10m_path = reproject_scl_layer_to_10_meter(bandSCL_path)

    boundary_file_names = find_boundary_file_names(data_dir, boundary_prefix)
    if not boundary_file_names:
        return
    
    # build the tiles ndvi map
    dst_crs = 'EPSG:4326'
    with tempfile.TemporaryDirectory() as tmpdir:
        # for each boundary stencil out the ndvi map and write the data
        # to a corresponding ndvi impage and meta files with the same id
        # boundary id format: f"{data_dir}/{boundary_prefix}_<boundary.ID>.json"
        for boundary_file_name in boundary_file_names:
            with rasterio.open(band04_path) as band04:
                utm_crs = band04.meta['crs']

            boundary_id = parse_boundary_id(boundary_file_name, boundary_prefix)
            boundary_shape_utm = build_boundary_shape(
                boundary_path=os.path.join(data_dir, boundary_file_name),
                utm_projection=utm_crs,
            )

            # compute the ndvi map
            with rasterio.open(band04_path) as band04:
                band04_data, _ = mask(band04, [boundary_shape_utm], crop=True)

            with rasterio.open(band08_path) as band08:
                band08_data, band08_masked_transform = mask(band08, [boundary_shape_utm], crop=True)
                band08_meta = band08.meta
            
            band08_data = band08_data.astype(float)
            band04_data = band04_data.astype(float)
            ndvi_map = (band08_data-band04_data)/(band08_data+band04_data)
            ndvi_map_meta = {**band08.meta}
            ndvi_map_meta["dtype"] = "float32"
            ndvi_map_meta["nodata"] = np.nan

            # compute the cloud mask when the SCL.tif layer has been included
            raster_percent_covered_by_clouds = None
            if bandSCL_10m_path:
                # this is a unsigned int 8 so only values from 0 to 255 are allowed
                # used 99 to represent data outside the boundary
                with rasterio.open(bandSCL_10m_path) as bandSCL:
                    bandSCL_data, _ = mask(bandSCL, [boundary_shape_utm], crop=True, nodata=99)

                # any cell not a cloud is set to zero
                # not exlucding label 10 as that is high serious cirrus clouds which probably
                # won't cause much distoring in the ndvi values
                bandSCL_data[(bandSCL_data != 8) & (bandSCL_data != 9) & (bandSCL_data != 99)] = 0

                # any cell that is a cloud is set to one
                bandSCL_data[(bandSCL_data > 0) & (bandSCL_data != 99)] = 1

                raster_percent_covered_by_clouds = (
                    float(bandSCL_data[bandSCL_data == 1].size) 
                    / float(bandSCL_data[bandSCL_data != 99].size)
                ) * 100.0

                # mask the ndvi map
                ndvi_map[(bandSCL_data == 1)] = np.nan
                
            # compute statistics to save to meta file
            valid_masked_data = ndvi_map[~np.isnan(ndvi_map)]
            if valid_masked_data.size == 0:
                print("masked ndvi map is empty; probably no data in the boundary")
                raster_min = 0.0
                raster_max = 0.0
                raster_mean = 0.0
                raster_median = 0.0
            else:
                raster_min = np.min(valid_masked_data)
                raster_max = np.max(valid_masked_data)
                raster_mean = np.mean(valid_masked_data)
                raster_median = np.median(valid_masked_data)

            boundary_ndvi_map_path = os.path.join(tmpdir, "boundary_ndvi_map.tiff")
            boundary_ndvi_map_meta = {**ndvi_map_meta}
            boundary_ndvi_map_meta["width"] = ndvi_map[0].shape[1]
            boundary_ndvi_map_meta["height"] = ndvi_map[0].shape[0]
            boundary_ndvi_map_meta["transform"] = band08_masked_transform
            with rasterio.open(boundary_ndvi_map_path, "w", **boundary_ndvi_map_meta) as src:
                src.write(ndvi_map[0], 1)

            boundary_web_mercator_ndvi_map_path = os.path.join(tmpdir, "boundary_web_mercator_ndvi_map.tiff")
            with rasterio.open(boundary_ndvi_map_path, "r") as src:
                boundary_ndvi_transform, width, height = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds
                )
                kwargs = src.meta.copy()
                kwargs.update({
                    'crs': dst_crs,
                    'transform': boundary_ndvi_transform,
                    'width': width,
                    'height': height
                })

                with rasterio.open(boundary_web_mercator_ndvi_map_path, 'w', **kwargs) as dst:
                    reproject(
                        source=rasterio.band(src, 1),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=boundary_ndvi_transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest)
            
            raster_image_path = os.path.join(data_dir, f"raster_image_{boundary_id}.png")
            raster_meta_path = os.path.join(data_dir, f"raster_meta_{boundary_id}.json")
            with rasterio.open(boundary_web_mercator_ndvi_map_path, "r") as src:
                image_meta = src.meta
                image_bounds = src.bounds
                norm = colors.Normalize(vmin=-1, vmax=1)
                image_color_data = np.uint8(colormaps.get_cmap("RdYlGn")(norm(src.read(1)))*255)
                ndvi_boundary_image = Image.fromarray(image_color_data)
                ndvi_boundary_image.save(raster_image_path)

                raster_meta = {
                    "imageBounds": [[float(image_bounds.bottom), float(image_bounds.left)], [float(image_bounds.top), float(image_bounds.right)]],
                    "rasterMin": round(float(raster_min), 8),
                    "rasterMax": round(float(raster_max), 8),
                    "rasterMedian": round(float(raster_median), 8),
                    "rasterMean": round(float(raster_mean), 8),
                    "rasterPercentCoveredByClouds": (
                        round(float(raster_percent_covered_by_clouds), 8) 
                        if raster_percent_covered_by_clouds is not None else None
                    ),
                }
                with open(raster_meta_path, "wb") as raster_meta_file:
                    raster_meta_file.write(json.dumps(raster_meta).encode("utf-8"))


def reproject_scl_layer_to_10_meter(scl_layer_path: str) -> str:

    if not scl_layer_path.endswith(".tif"):
        raise ValueError()

    scl_layer_name = scl_layer_path.rsplit('.tif', 1)[0]
    scl_10m_path = f"{scl_layer_name}_10m.tif"

    with rasterio.open(scl_layer_path, "r") as screen_segmentation_layer:
        screen_segmentation_layer_data = screen_segmentation_layer.read(
            1,
            out_shape=(
                int(screen_segmentation_layer.height * 2),
                int(screen_segmentation_layer.width * 2)
            ),
            resampling=Resampling.bilinear
        )

        dst_transform = screen_segmentation_layer.transform * screen_segmentation_layer.transform.scale(
            (screen_segmentation_layer.width / screen_segmentation_layer_data.shape[-1]),
            (screen_segmentation_layer.height / screen_segmentation_layer_data.shape[-2])
        )

        dst_kwargs = screen_segmentation_layer.meta.copy()
        dst_kwargs.update(
            {
                "transform": dst_transform,
                "width": screen_segmentation_layer_data.shape[1],
                "height": screen_segmentation_layer_data.shape[0],
                "nodata": 0,
            }
        )

        with rasterio.open(scl_10m_path, "w", **dst_kwargs) as dst:
            dst.write(screen_segmentation_layer_data, 1)

    return scl_10m_path


if __name__ == "__main__":
    print("computing ndvi maps...")

    parser = argparse.ArgumentParser(description="Process NDVI maps using boundaries")
    parser.add_argument("data_dir", metavar="DATA_DIRECTORY", type=str, nargs="?",
                    help="the directory to read and write data to")
    parser.add_argument(
        "band_prefix", metavar="BAND_PREFIX", type=str, nargs="?",
        help="the prefix of the band data files",
    )
    parser.add_argument(
        "boundary_prefix", metavar="BOUNDARY_PREFIX", type=str, nargs="?",
        help="the prefix of the boundary geojson files",
    )
    args = parser.parse_args()

    if not args.data_dir or not args.band_prefix or not args.boundary_prefix:
        print("missing argument data")
        exit()

    try:
        build_ndvi_maps_for_boundaries(
            data_dir=args.data_dir,
            band_prefix=args.band_prefix,
            boundary_prefix=args.boundary_prefix,
        )
    except Exception as err:
        print("had an error")
        print(err)

    print("maps computed and written to the data directory")
