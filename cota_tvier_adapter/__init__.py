import csv
import gzip
import io
import json
import re
import tarfile
import zipfile
from xml.etree.ElementTree import fromstring

import requests
from fastapi import FastAPI
from xmljson import parker

app = FastAPI()

event_map = {"receivedBSM": "BSM", "sentBSM": "BSM"}


@app.get("/api/v1/tvier")
async def do_tvier(url: str):
    print("grabbing from url", url)
    response = requests.get(url)

    stuff = io.BytesIO(response.content)
    # print(zipfile.is_zipfile(stuff))
    rows = []
    search = re.compile(r"_obu_(.*?)\.")
    with zipfile.ZipFile(stuff) as zippy:
        for zoopy in zippy.infolist():
            source_device = re.search(search, zoopy.filename).group(1)
            # print('JALSON', source_device)
            sterf = io.BytesIO(zippy.read(zoopy))
            with gzip.open(sterf, mode="rt") as gunzippy:
                stoff = csv.DictReader(gunzippy)
                obj = {}
                for row in stoff:
                    event_type = row["EventType"]
                    if event_type not in ["sentBSM", "receivedBSM"]:
                        continue
                    event_data = parker.data(fromstring(row["EventData"]))
                    # print('JALSON', event_data)
                    message_body = list(
                        event_data["message"]["MessageFrame"]["value"].values()
                    )[0]
                    obj["timestamp"] = row["Timestamp"]
                    obj["sourceDevice"] = source_device
                    obj["messageType"] = event_map[event_type]
                    obj["messageBody"] = message_body
                    rows.append(obj)
        # with myzip.open('eggs.txt') as myfile:
        #     print(myfile.read())
    # tarnation = tarfile.open(fileobj=stuff, mode="r|*")

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
    return rows


if __name__ == "__main__":
    app
