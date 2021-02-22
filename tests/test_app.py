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
    fixture_date = "2021_01_01"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.gz&hour=00")

    assert response.status_code == 200
    print(response.text[0:100])
    f = StringIO(response.text)
    reader = csv.DictReader(f, fieldnames=['sourceDevice', 'timestamp', 'messageType', 'messageBody'])
    rows = []
    for row in reader:
        row['messageBody'] = json.loads(row['messageBody'])
        rows.append(row)

    assert {
        "sourceDevice": "2520203122717008050",
        "timestamp": "01/01/2021 03:06:59.190",
        "messageType": "sentBSM",
        "messageBody": {
            "coreData": {
                "msgCnt": 66,
                "id": "6A9D5153",
                "secMark": 59150,
                "lat": 399650051,
                "long": -830397670,
                "elev": 1847,
                "accuracy": {
                    "semiMajor": 9,
                    "semiMinor": 8,
                    "orientation": 19195
                },
                "transmission": {
                    "unavailable": None
                },
                "speed": 0,
                "heading": 21987,
                "angle": 127,
                "accelSet": {
                    "long": -6,
                    "lat": 5,
                    "vert": 49,
                    "yaw": 0
                },
                "brakes": {
                    "wheelBrakes": 10000,
                    "traction": {
                        "unavailable": None
                    },
                    "abs": {
                        "unavailable": None
                    },
                    "scs": {
                        "unavailable": None
                    },
                    "brakeBoost": {
                        "off": None
                    },
                    "auxBrakes": {
                        "unavailable": None
                    }
                },
                "size": {
                    "width": 290,
                    "length": 1240
                }
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
                                            "latOffset": 366,
                                            "lonOffset": -3451,
                                            "elevationOffset": -4,
                                            "timeOffset": 2950
                                        },
                                        {
                                            "latOffset": 890,
                                            "lonOffset": -4559,
                                            "elevationOffset": -4,
                                            "timeOffset": 3235
                                        },
                                        {
                                            "latOffset": 1711,
                                            "lonOffset": -5049,
                                            "elevationOffset": -4,
                                            "timeOffset": 3520
                                        },
                                        {
                                            "latOffset": 9167,
                                            "lonOffset": -4372,
                                            "elevationOffset": 18,
                                            "timeOffset": 7690
                                        },
                                        {
                                            "latOffset": 11331,
                                            "lonOffset": -3892,
                                            "elevationOffset": 24,
                                            "timeOffset": 8190
                                        },
                                        {
                                            "latOffset": 12236,
                                            "lonOffset": -3062,
                                            "elevationOffset": 27,
                                            "timeOffset": 8410
                                        },
                                        {
                                            "latOffset": 12748,
                                            "lonOffset": -1723,
                                            "elevationOffset": 27,
                                            "timeOffset": 8620
                                        },
                                        {
                                            "latOffset": 12619,
                                            "lonOffset": 4264,
                                            "elevationOffset": 23,
                                            "timeOffset": 9405
                                        }
                                    ]
                                }
                            },
                            "pathPrediction": {
                                "radiusOfCurve": 32767,
                                "confidence": 200
                            }
                        }
                    }
                }
            }
        }
    } in rows


def test_hour_not_found(fake_content_server_url):
    fixture_url = fake_content_server_url + "/cota_obu_data"
    fixture_date = "2021_01_01"
    response = client.get(f"/api/v1/tvier?url={fixture_url}_{fixture_date}.gz&hour=66")

    assert response.status_code == 200
    assert response.text == ""
