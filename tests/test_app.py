import json
from io import StringIO
import csv

import pytest
from fastapi.testclient import TestClient

from cota_tvier_adapter import app
from tests.fake_cota_content_server import create_fake_server

client = TestClient(app)


@pytest.fixture(scope="module")
def fake_content_server_url():
    return create_fake_server()


def test_high_level(fake_content_server_url):
    fixture_url = fake_content_server_url + "/cota_obu_data"
    fixture_date = "2021_04_12"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.gz&hour=00")

    assert response.status_code == 200
    print(response.text[0:100])
    f = StringIO(response.text)
    reader = csv.DictReader(
        f, fieldnames=["sourceDevice", "timestamp", "messageType", "messageBody"]
    )
    rows = []
    test_row = ""
    for row in reader:
        row["messageBody"] = json.loads(row["messageBody"])
        if row["messageType"] == "sentBSM":
            test_row = row
        rows.append(row)

    print(test_row)

    assert {
        "sourceDevice": "252020312271700794",
        "timestamp": "04/12/2021 02:57:42.597",
        "messageType": "sentBSM",
        "messageBody": {
            "coreData": {
                "msgCnt": 36,
                "id": "03C4FF55",
                "secMark": 42550,
                "lat": 399650416,
                "long": -830401267,
                "elev": 1828,
                "accuracy": {"semiMajor": 16, "semiMinor": 15, "orientation": 14137},
                "transmission": {"unavailable": None},
                "speed": 46,
                "heading": 22023,
                "angle": 127,
                "accelSet": {"long": 18, "lat": 24, "vert": 49, "yaw": -5},
                "brakes": {
                    "wheelBrakes": 10000,
                    "traction": {"unavailable": None},
                    "abs": {"unavailable": None},
                    "scs": {"unavailable": None},
                    "brakeBoost": {"off": None},
                    "auxBrakes": {"unavailable": None},
                },
                "size": {"width": 290, "length": 1240},
            },
            "partII": {
                "SEQUENCE": {
                    "partII-Id": 0,
                    "partII-Value": {
                        "VehicleSafetyExtensions": {
                            "pathHistory": {
                                "crumbData": {
                                    "PathHistoryPoint": [
                                        {
                                            "latOffset": -5,
                                            "lonOffset": -35,
                                            "elevationOffset": -1,
                                            "timeOffset": 20,
                                        },
                                        {
                                            "latOffset": 82,
                                            "lonOffset": -3125,
                                            "elevationOffset": -7,
                                            "timeOffset": 17070,
                                        },
                                        {
                                            "latOffset": 212,
                                            "lonOffset": -3124,
                                            "elevationOffset": -17,
                                            "timeOffset": 31755,
                                        },
                                        {
                                            "latOffset": 369,
                                            "lonOffset": -7168,
                                            "elevationOffset": -28,
                                            "timeOffset": 36635,
                                        },
                                        {
                                            "latOffset": 804,
                                            "lonOffset": -8171,
                                            "elevationOffset": -27,
                                            "timeOffset": 36920,
                                        },
                                        {
                                            "latOffset": 1737,
                                            "lonOffset": -8684,
                                            "elevationOffset": -25,
                                            "timeOffset": 37235,
                                        },
                                        {
                                            "latOffset": 10235,
                                            "lonOffset": -7903,
                                            "elevationOffset": -18,
                                            "timeOffset": 40495,
                                        },
                                        {
                                            "latOffset": 11728,
                                            "lonOffset": -7366,
                                            "elevationOffset": -14,
                                            "timeOffset": 40775,
                                        },
                                        {
                                            "latOffset": 12484,
                                            "lonOffset": -6599,
                                            "elevationOffset": -10,
                                            "timeOffset": 40975,
                                        },
                                    ]
                                }
                            },
                            "pathPrediction": {
                                "radiusOfCurve": 32767,
                                "confidence": 200,
                            },
                        }
                    },
                }
            },
        },
    } in rows


def test_hour_not_found(fake_content_server_url):
    fixture_url = fake_content_server_url + "/cota_obu_data"
    fixture_date = "2021_04_12"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.gz&hour=66")

    assert response.status_code == 200
    assert response.text == ""
