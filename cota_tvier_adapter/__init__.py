"""
This is the controller.
"""
import csv
import gzip
import io
import json
import re
import tarfile
import zipfile
import zlib
from functools import reduce

import requests
import uvicorn
from fastapi import FastAPI

from cota_tvier_adapter.record_converter import convert_row

app = FastAPI()

search = re.compile(r"_obu_(.*?)\.")


@app.get("/api/v1/tvier")
def tvier(url: str):
    """
    Downloads a gzip archive from the provided url and extracts data from the contained csv files
    """
    response = requests.get(url)
    archive_bytes = io.BytesIO(response.content)
    return _process_zip_archive(archive_bytes)


@app.get("/api/v1/healthcheck")
def healthcheck():
    return "Ok"


def _process_tar_archive(tar_bytes):
    with tarfile.open(fileobj=tar_bytes) as tar_file:
        members = tar_file.getmembers()
        return reduce(
            lambda rows, tar_info: _extract_gzips(rows, tar_info, tar_file), members, []
        )


def _process_zip_archive(zip_bytes):
    with zipfile.ZipFile(zip_bytes) as zip_file:
        members = zip_file.infolist()
        return reduce(
            lambda rows, zip_info: _extract_gzip_from_zip(rows, zip_info, zip_file),
            members,
            [],
        )


def _extract_gzips(rows, tar_info, tar_file):
    source_device = re.search(search, tar_info.name).group(1)
    gzip_file = tar_file.extractfile(tar_info)
    extracted_records = _process_gzip(gzip_file, source_device)
    return rows + extracted_records


def _extract_gzip_from_zip(rows, zip_info, zip_file):
    source_device = re.search(search, zip_info.filename).group(1)
    gzip_file = zip_file.open(zip_info)

    try:
        unzipped = zlib.decompress(gzip_file.read(), wbits=16).decode("utf-8").splitlines()
        extracted_records = _process_csv_file(unzipped, source_device)
        return rows + extracted_records
    except:
        print(f"Could not process file: {zip_info.filename}")
        return rows


def _process_gzip(gzip_file, source_device):
    with gzip.open(gzip_file, mode="rt") as csv_file:
        _process_csv_file(csv_file, source_device)

def _process_csv_file(csv_file, source_device):
    data = csv.DictReader(
        csv_file,
        fieldnames=[
            "Timestamp",
            "TimeUTC",
            "AccOnSession",
            "EventType",
            "Latitude",
            "Longitude",
            "Intersection",
            "EventData",
        ],
    )
    converted_data = reduce(convert_row, data, [])
    return list(
        map(lambda row: _add_source_device(row, source_device), converted_data)
    )


def _add_source_device(row, source_device):
    row.update({"sourceDevice": source_device})
    return row


if __name__ == "__main__":
    uvicorn.run("cota_tvier_adapter:app", host="127.0.0.1", port=5000, log_level="info")
