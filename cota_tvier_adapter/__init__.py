import csv
import gzip
import io
import json
import re
import tarfile
import zipfile
from xml.etree.ElementTree import fromstring
from functools import reduce

import requests
from fastapi import FastAPI
from xmljson import parker

app = FastAPI()

event_map = {"receivedBSM": "BSM", "sentBSM": "BSM"}

search = re.compile(r"_obu_(.*?)\.")


@app.get("/api/v1/tvier")
async def do_tvier(url: str):
    print("grabbing from url", url)
    response = requests.get(url)
    archive_bytes = io.BytesIO(response.content)
    return _process_zip_archive(archive_bytes)

    # gunzip file
    # for each csv in folder output
    # gunzip as a csv
    # for each csv
    ## extract obu from filename as "sourceDevice"
    ## decode into list of dicts
    ## for each dict in list
    ### extract Timestamp as "timestamp"
    ### extract EventType as "messageType"
    ### transcode EventData => dict
    ### extract nested BSM/whatever as "messageBody"
    # print(json.dumps(rows[0]))


def _process_zip_archive(zip_bytes):
    with zipfile.ZipFile(zip_bytes) as zip_archive:
        return reduce(lambda rows, zip_info: _extract_gzips(rows, zip_info, zip_archive), zip_archive.infolist(), [])


def _extract_gzips(rows, zip_info, zip_archive):
    source_device = re.search(search, zip_info.filename).group(1)
    gzip_bytes = zip_archive.read(zip_info)
    extracted_records = _process_gzip(gzip_bytes, source_device)
    return rows + extracted_records


def _process_gzip(gzip_bytes, source_device):
    gzip_file = io.BytesIO(gzip_bytes)
    with gzip.open(gzip_file, mode="rt") as csv_file:
        data = csv.DictReader(csv_file)
        return reduce(lambda rows, row: _convert_row(rows, row, source_device), data, [])


def _convert_row(rows, row, source_device):
    event_type = row["EventType"]
    if event_type not in ["sentBSM", "receivedBSM"]:
        return rows
    
    obj = {}
    event_data = parker.data(fromstring(row["EventData"]))
    message_body = list(
        event_data["message"]["MessageFrame"]["value"].values()
    )[0]
    obj["timestamp"] = row["Timestamp"]
    obj["sourceDevice"] = source_device
    obj["messageType"] = event_map[event_type]
    obj["messageBody"] = message_body
    rows.append(obj)
    return rows

if __name__ == "__main__":
    app
