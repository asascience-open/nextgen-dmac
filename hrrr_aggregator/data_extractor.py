"""
# Proof of concept to build Zarr aggregations on the fly


Tools to work grib/zarr data. Assumes singleton time dimension but could be generalized to any
dataset where chunks are per time slice.


MIT License Copyright (c) 2023 Camus Energy

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import logging
import os

import msgpack
import io
import re
import base64

import numpy as np
import zarr
import ujson
import fsspec

import pandas as pd
import xarray as xr

from collections import defaultdict

from typing import Optional, Any, Literal
from kerchunk.grib2 import scan_grib

logger = logging.getLogger(__name__)


class DataExtractorError(ValueError):
    pass


ZCHUNK_MATCH = re.compile(r"^(?P<name>.*)\/(?P<zchunk>\d+[\.\d+]*)$")


COLUMN_NAMES = [
    "name",
    "forecast_valid_time",
    "forecast_run_time",
    "model_horizon",
    "zchunk",
    "uri",
    "offset",
    "length",
    "stepType",
    "typeOfLevel",
    "layer",
]

# CONSTANTS
ZARR_VERSION = 2
KERCHUNK_VERSION = 1
COORDINATES_FILE = "zarr_stored_coordinates.grib2"  # Doens't have to be grib?
ZARR_TREE_STORE = "zarr_tree_store.msgpack"


def build_path(path: list[str | None], suffix: Optional[str] = None):
    return "/".join([val for val in [*path, suffix] if val is not None])


def copy_group_metadata(source: dict, dest: dict, path: list[str | None]):
    attrs_path = build_path(path, ".zattrs")
    if attrs_path in source:
        dest[attrs_path] = source[attrs_path]

    group_path = "/".join([*path, ".zgroup"])
    dest[group_path] = source[group_path]


def add_chunks_to_zarr(
    chunks: pd.DataFrame,
    zarr_store: dict,
    layers: list[str],
    method: Literal["model_horizon", "forecast_runtime", "forecast_valid_time"],
    axes: list[pd.Index],
) -> dict:
    """
    Given a zarr_store hierarchy, pull out the variables present in the chunks dataframe and reinflate the zarr
    variables adding any needed dimensions

    Current implementation is more proof of concept that fully functional implementation...
    :param chunks:
    :param zarr_store:
    :param layers: the ordered names of any layers used in hierarchical groups
    :param axes: a list of new axes for aggregation
    :return: the inflated zarr store
    """
    if not (chunks.columns == COLUMN_NAMES).all():
        raise ValueError(
            "grib_chunks dataframe does not have the expected columns: Got %s",
            chunks.columns,
        )

    result = {}
    axes_by_name: dict[str, pd.Index] = {pdi.name: pdi for pdi in axes}

    # Copy the root group entry and any metadata
    copy_group_metadata(zarr_store, result, [])

    # Copy all the groups that contain variables in the chunk dataset
    unique_groups = chunks.set_index(["stepType", "typeOfLevel"]).index.unique()

    dt = axes_by_name["forecast_valid_time"].to_numpy().astype("datetime64[s]")
    dt_store = {}
    zdt = zarr.array(dt, store=dt_store)
    zdt.attrs.update(
        {
            "_ARRAY_DIMENSIONS": ["forecast_valid_time"],
            "calendar": "proleptic_gregorian",
            "long_name": "time",
            "standard_name": "time",
            "units": "seconds since 1970-01-01T00:00:00",
        }
    )

    # TODO add forecst runtime and model horizon

    lvals = []
    for layer in layers:
        lvals.append(layer)
        for path in zip(
            *[unique_groups.get_level_values(lval).tolist() for lval in lvals]
        ):
            copy_group_metadata(zarr_store, result, path)

            # Cheat for now - use known coordinate name
            pname = build_path(path, "latitude/.zattrs")
            if pname in zarr_store:
                result[pname] = zarr_store[pname]
                pname = build_path(path, "latitude/.zarray")
                result[pname] = zarr_store[pname]
                pname = build_path(path, "latitude/0.0")
                result[pname] = zarr_store[pname]

                pname = build_path(path, "longitude/.zattrs")
                result[pname] = zarr_store[pname]
                pname = build_path(path, "longitude/.zarray")
                result[pname] = zarr_store[pname]
                pname = build_path(path, "longitude/0.0")
                result[pname] = zarr_store[pname]

                for key, val in dt_store.items():
                    valid_time_path = build_path(path, "forecast_valid_time")
                    result[f"{valid_time_path}/{key}"] = val

    key_layer_index = dict(stepType=1, typeOfLevel=2)
    chunk_groups = chunks.groupby(by=["name", "stepType", "typeOfLevel", "layer"])
    for key, group in chunk_groups:
        # TODO set other non aggregation dimensions
        indexed_chunks = (
            group.set_index("forecast_valid_time")
            .reindex(axes_by_name["forecast_valid_time"], method=None, fill_value=0)
            .reset_index()
        )

        path = build_path([key[key_layer_index[layer]] for layer in layers], key[0])

        zattrs = f"{path}/.zattrs"
        za_data = ujson.loads(zarr_store[zattrs])
        za_data["_ARRAY_DIMENSIONS"] = [
            "forecast_valid_time",
            *za_data["_ARRAY_DIMENSIONS"],
        ]
        result[zattrs] = ujson.dumps(za_data)

        zarray = f"{path}/.zarray"
        zr_data = ujson.loads(zarr_store[zarray])
        zr_data["chunks"] = [1, *zr_data["chunks"]]
        zr_data["shape"] = [len(indexed_chunks), *zr_data["shape"]]
        result[zarray] = ujson.dumps(zr_data)

        for ind, row in indexed_chunks.iterrows():
            if row.uri != 0:
                zchunk = f"{path}/{ind}.{row.zchunk}"
                result[zchunk] = [row.uri, row.offset, row.length]

    return result


def scan_grib_to_groups(
    uri: str,
    storage_options: Optional[dict] = None,
    inline_threshold: int = 10,
) -> list[tuple[dict, dict]]:

    # No filters,  - get all the messages!
    return [
        partial_decode_kerchunk(message_metadata)
        for message_metadata in scan_grib(
            uri, storage_options=storage_options, inline_threshold=inline_threshold
        )
    ]


def copy_zarr_dataset(vname: str, zgroup: dict, group: zarr.Group):
    """
    Copy the dataset metadata for the variable vname from the zgroup zarr store into the zarr.Group object.
    :param vname: the variable name
    :param zgroup: the zarr store to copy from
    :param group: the zarr Gropu to copy into
    :return:
    """
    # TODO I think this could be done purely by copying the .zarray and .zattrs between the stores and mangling the path in the keys?
    zarray = zarr.open_array({".zarray": ujson.loads(zgroup[f"{vname}/.zarray"])})
    zattrs = ujson.loads(zgroup[f"{vname}/.zattrs"])
    dset = group.create_dataset(
        name=vname,
        **{
            key: getattr(zarray, key)
            for key in [
                "chunks",
                "compressor",
                "dtype",
                "fill_value",
                "filters",
                "order",
                "shape",
            ]
        },
    )
    dset.attrs.update(**zattrs)
    logger.info("copied %s to dset: %s", vname, dset)


def copy_reference(
    kerchunk: tuple[str, int, int],
    state_map: dict[tuple[str, int, int] : tuple[str, int, int]],
    path: str,
    bfile: io.BytesIO,
):
    """
    Helper method to copy the data from a kerchunk reference into a new binary file and return an updated
    kerchunk tuple for the new path. The state map can prevent duplication for the same kerchunk tuple.
    :param kerchunk: the tuple pointing to bytes in the original data source
    :param state_map: a map from the original source kerchunks to new the kerchunk data copies in the new file
    :param path:
    :return:
    """
    if kerchunk in state_map:
        return state_map[kerchunk]

    length = kerchunk[2]
    with fsspec.open(kerchunk[0], "rb") as f:
        f.seek(kerchunk[1])
        bytes = f.read(length)

    offset = bfile.seek(0, 2)  # end of file
    bfile.write(bytes)
    new_kerchunk = (path, offset, length)
    state_map[kerchunk] = new_kerchunk
    return new_kerchunk


def treeify_groups(
    zgroups: list[dict[str, Any], dict[dict[tuple[str, int, int]]]],
    layers: list[str],
    metadata_path: str,
    coords_file: io.BytesIO,
    forecast_valid_time: str = "forecast_valid_time",
    model_horizon: str = "model_horizon",
    forecast_run_time: str = "forecast_run_time",
    select_valid_time: Optional[np.datetime64] = None,
) -> dict:
    """
    Given a list of partially decoded kerchunk group dictionaries, read them enough to reason about the structure
    and content and build a zarr hierarchy based on the layer names as needed.
    This function writes both the zarr store hierarchy and a new binary data file for the constant coordinate
    variables to the given path.
    The path needs to be given in the input to make the correct kerchunk references the write operation could be
    pulled out of the function. Using a kerchunk template could remote the path from the function api as well.
    :param zgroups: a list of partially decoded zarr group stores and chunks
    :param layers: the ordered names of any layers that should be used to create hierarchical groups
    :param metadata_path: the path (local or cloud store) to use when creating the dataset artifact metadata
    :param forecast_valid_time: coordinate name in the grib file of the forecast_valid_time
    :param model_horizon: coordinate name in the grib file of the model_horizon
    :param forecast_run_time: coordinate name in the grib file of the forecast_run_time
    :param select_valid_time: If there is more than one valid time in the zgroups, filter on the selected one when
    constructing the zarr hierarchy
    :return: the zarr hierarchy
    """
    store = {}
    result = zarr.open_group(store=store)

    # For copying existing lat lon refs in a grib2 file to our metadata store
    zarr_coordinates_path = os.path.join(metadata_path, COORDINATES_FILE)
    coordinate_chunk_copy_state: dict[tuple[str, int, int] : tuple[str, int, int]] = {}

    unknown_counter = 0
    known_counter = 0
    # TODO add error handling
    for zgroup, kerchunks in zgroups:

        xd = xr.open_dataset(zgroup, engine="zarr", consolidated=False)
        for vname, dvar in xd.data_vars.items():
            if vname == "unknown":
                unknown_counter += 1
                continue
            known_counter += 1

            if select_valid_time is not None:
                vt = xd[forecast_valid_time].values[()]
                if vt != select_valid_time:
                    logger.debug("Skipping var %s for valid time %s", vname, vt)
                    continue

            dset_group = result
            for field in layers:
                layer = dvar.attrs.get(f"GRIB_{field}")
                if layer is not None:
                    dset_group = dset_group.require_group(layer)
                    dset_group.attrs[field] = layer

            if vname in dset_group:
                logger.warning(
                    "Group %s already contains an array %s", dset_group, vname
                )
                continue

            zattrs = ujson.loads(zgroup[".zattrs"])
            dset_group.attrs.update(**zattrs)
            # TODO: check for changes? Do something smart?

            # Copy the variable from the kerchunked zarr group to the new zarr group hierarchy
            copy_zarr_dataset(vname, zgroup, dset_group)

            # Skip the time coordinates or coordinates already in the group
            for cname in dvar.coords.keys():
                logger.debug(
                    "Vname %s, dset_group arrays: %s",
                    vname,
                    [k for k in dset_group.array_keys()],
                )
                if (
                    cname in [forecast_run_time, forecast_valid_time, model_horizon]
                ) or (cname in dset_group):
                    continue

                copy_zarr_dataset(cname, zgroup, dset_group)

                if cname in kerchunks:
                    for chunk_index, ref_tuple in kerchunks[cname].items():
                        # Copy all the constant coordinate variables to a new file
                        # This way all the constant data lives in one place rather than keeping references
                        # to whichever dataset what used to generate the zarr store hierarchy
                        new_ref_tuple = copy_reference(
                            ref_tuple,
                            coordinate_chunk_copy_state,
                            zarr_coordinates_path,
                            coords_file,
                        )
                        logger.info(
                            "Coordinate %s reference file state: %s",
                            vname,
                            coordinate_chunk_copy_state,
                        )
                        result.store[
                            f"{dset_group.path}/{cname}/{chunk_index}"
                        ] = new_ref_tuple

                ## You could copy the 1d or 0d coordinate values now,
                ## but they should be stored with the chunk references as they can have multiple values
                # else:
                #     dset_group[cname][...] = dvar[cname][...]

    return store


def write_store(metadata_path: str, store: dict):
    fpath = os.path.join(metadata_path, ZARR_TREE_STORE)
    packedb = msgpack.packb(store)
    with fsspec.open(fpath, "wb") as f:
        f.write(packedb)

    logger.info("Wrote %d bytes to %s", len(packedb), fpath)


def read_store(metadata_path: str):
    fpath = os.path.join(metadata_path, ZARR_TREE_STORE)
    with fsspec.open(fpath, "rb") as f:
        packedb = f.read()
    logger.info("Read %d bytes to %s", len(packedb), fpath)
    zarr_store = msgpack.unpackb(packedb)
    return zarr_store


# Don't need a read for the coords - zarr/fsspec will do the reading
def write_coords(metadata_path: str, coords_bio: io.BytesIO):
    coords_bio.seek(0)
    fpath = os.path.join(metadata_path, COORDINATES_FILE)
    with fsspec.open(fpath, "wb") as f:
        bbytes = coords_bio.read()
        f.write(bbytes)
    logger.info("Wrote %d bytes to %s", len(bbytes), fpath)


# Gross api, but it should be pretty efficient
def extract_chunks(
    zgroups: list[dict[str, Any], dict[dict[tuple[str, int, int]]]],
    layers: list[str],
    forecast_valid_time: str = "forecast_valid_time",
    model_horizon: str = "model_horizon",
    forecast_run_time: str = "forecast_run_time",
) -> pd.DataFrame:
    """
    Extract the grib chunk data for data variables into an in memory pandas dataframe
    :param zgroups: a list of partially decoded kerchunk objects
    :param layers: a list of the layer names to look for in the GRIB attributes
    :param forecast_valid_time: coordinate name in the grib file of the forecast_valid_time
    :param model_horizon: coordinate name in the grib file of the model_horizon
    :param forecast_run_time: coordinate name in the grib file of the forecast_run_time
    :return: a dataframe with kerchunk references and their associated coordinate/layer data
    """
    result: list[dict] = []

    unknown_counter = 0
    known_counter = 0
    # TODO add error handling
    for zgroup, kerchunks in zgroups:

        # Using xarray is probably not great for performance - but I don't want to reimplement it either
        xd = xr.open_dataset(zgroup, engine="zarr", consolidated=False)
        for vname, dvar in xd.data_vars.items():
            if vname == "unknown":
                unknown_counter += 1
                continue
            known_counter += 1

            layers_vals = {field: dvar.attrs.get(f"GRIB_{field}") for field in layers}

            vt = xd[forecast_valid_time].values[()]
            ref_time = xd[forecast_run_time].values[()]
            if hasattr(xd, model_horizon):
                horizon = xd[model_horizon].values[()]
            else:
                horizon = vt - ref_time

            coords = {}
            for layer_name in layers_vals.values():
                if layer_name in dvar.coords:
                    coords["layer"] = xd[layer_name].values[()]

            for zchunk, lookup in kerchunks[vname].items():
                result.append(
                    dict(
                        name=vname,
                        forecast_valid_time=vt,
                        forecast_run_time=ref_time,
                        model_horizon=horizon,
                        zchunk=zchunk,
                        uri=lookup[0],
                        offset=lookup[1],
                        length=lookup[2],
                        **layers_vals,
                        **coords,
                    )
                )

    logger.info("Found %d chunks for %d known variables", len(result), known_counter)
    if unknown_counter > 0:
        logger.warning("Found %d unknown variables", unknown_counter)

    return pd.DataFrame.from_records(result)


def decode_kerchunk(
    chunk: str | bytes | list, template_data: dict[str, str]
) -> bytes | tuple[str, int, int]:
    """
    Decoder for the kerchunk zarr string bytes or tuple
    It applies the template and returns either bytes or tuple(uri, offset, length)
    :param chunk:
    :param template_data:
    :return:
    """
    if isinstance(chunk, list):
        assert len(chunk) == 3, "Grib chunk list must be length 3"
        uri, offset, length = chunk
        return (
            # Double format_map because the template variable looks like "{{u}}"?
            uri.format_map(template_data).format_map(template_data),
            offset,
            length,
        )
    elif isinstance(chunk, bytes):
        return chunk
    elif isinstance(chunk, str):
        if chunk.startswith("base64:"):
            return base64.b64decode(chunk.lstrip("base64:"))
        else:
            return chunk.encode()


def partial_decode_kerchunk(
    kerchunk_data: dict,
) -> [dict[str, Any], dict[dict[tuple[str, int, int]]]]:
    """
    Replace the chunk references with the full uri using the template and decode base64 values.
    Effectively splits the dataset into variables that are large kerchunk references and small zero/one dimensional
    coordinates
    :param kerchunk_data: the group object returned by kerchunk'ing a dateset or grib message
    :return: a tuple of mappings that can be used to hack zarr/xarray
    """
    # Extract the top level "refs" from the dict returned by scangrib
    refs = kerchunk_data["refs"]
    # fsspec format version
    version = kerchunk_data["version"]
    if version != 1:
        raise ValueError("Unexpected kerchunk message version: %s", version)

    template_data = kerchunk_data.get("templates")

    # Use some fancy regex to help understand the zarr/kerchunk chunks
    result_metadata = {}
    kerchunks = defaultdict(dict)
    for key, val in refs.items():
        rematch = re.search(ZCHUNK_MATCH, key)
        if rematch:
            decoded = decode_kerchunk(val, template_data=template_data)
            if isinstance(decoded, bytes):
                result_metadata[key] = decoded
            elif isinstance(decoded, tuple):
                kerchunks[rematch["name"]][rematch["zchunk"]] = decoded
        else:
            result_metadata[key] = val

    return result_metadata, kerchunks
