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
    return _process_tar_archive(archive_bytes)


def _process_tar_archive(tar_bytes):
    with tarfile.open(fileobj=tar_bytes) as tar_file:
        members = tar_file.getmembers()
        return reduce(
            lambda rows, tar_info: _extract_gzips(rows, tar_info, tar_file), members, []
        )


def _extract_gzips(rows, tar_info, tar_file):
    source_device = re.search(search, tar_info.name).group(1)
    gzip_file = tar_file.extractfile(tar_info)
    extracted_records = _process_gzip(gzip_file, source_device)
    return rows + extracted_records


def _process_gzip(gzip_file, source_device):
    with gzip.open(gzip_file, mode="rt") as csv_file:
        data = csv.DictReader(csv_file)
        converted_data = reduce(convert_row, data, [])
        return list(
            map(lambda row: _add_source_device(row, source_device), converted_data)
        )


def _add_source_device(row, source_device):
    row.update({"sourceDevice": source_device})
    return row


if __name__ == "__main__":
    uvicorn.run("cota_tvier_adapter:app", host="127.0.0.1", port=5000, log_level="info")
