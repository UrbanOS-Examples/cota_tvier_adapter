"""
This is the controller.
"""
import csv
import gzip
import io
import json
import re
import zipfile
import zlib
import pickle
import logging
import requests
import uvicorn

from retry import retry
from fastapi import FastAPI
from starlette.responses import StreamingResponse
from cota_tvier_adapter.record_converter import convert_row

app = FastAPI()

search = re.compile(r"_obu_(.*?)\.")


@app.get("/api/v1/tvier")
def tvier(url: str, hour: str):
    """
    Downloads a gzip archive from the provided url and extracts data from the contained csv files
    """
    response = requests.get(url)
    archive_bytes = io.BytesIO(response.content)
    record_generator = _stream_records_from_archive(archive_bytes, hour)
    return StreamingResponse(record_generator, media_type='application/json')


@app.get("/api/v1/healthcheck")
def healthcheck():
    return "Ok"

def _stream_records_from_archive(archive_bytes, hour):
    yield "[\n"
    for rows in _process_zip_archive(archive_bytes, hour):
        for row in rows:
            yield json.dumps(row) + ',\n'
    yield '[]]' # Required due to trailing commas. Will be dead-lettered.

@retry(tries=3, delay=10, backoff=10)
def _process_zip_archive(zip_bytes, hour):
    with zipfile.ZipFile(zip_bytes) as zip_file:
        members = zip_file.infolist()
        for member in members:
            if member.filename.split('_')[2] == hour:
                print(f"Extracting matching file: {member.filename}")
                yield _extract_gzip_from_zip(member, zip_file)


def _extract_gzip_from_zip(zip_info, zip_file):
    logging.info(f"Decompressing {zip_info.filename}")
    source_device = re.search(search, zip_info.filename).group(1)
    gzip_file = zip_file.open(zip_info)

    try:
        unzipped = zlib.decompress(gzip_file.read(), wbits=16).decode("utf-8").splitlines()
        extracted_records = _process_csv_file(unzipped, source_device)
        return extracted_records
    except:
        logging.warning(f"Could not process file: {zip_info.filename}")
        return None


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

    for record in data:
        converted_record = convert_row(record)
        if converted_record != None: 
            yield _add_source_device(converted_record, source_device)


def _add_source_device(row, source_device):
    row.update({"sourceDevice": source_device})
    return row


if __name__ == "__main__":
    uvicorn.run("cota_tvier_adapter:app", host="127.0.0.1", port=5000, log_level="info")
