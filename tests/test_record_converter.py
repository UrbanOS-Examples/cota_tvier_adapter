import pytest
from cota_tvier_adapter.record_converter import convert_row


def test_basic_safety_message():
    timestamp = "7/22/2020  9:20:28 AM"
    message_type = "sentBSM"
    row = {"EventType": message_type, "EventData": _bsm(), "Timestamp": timestamp}

    [row] = convert_row([], row)

    assert row["messageBody"]["coreData"]
    assert row["timestamp"] == timestamp
    assert row["messageType"] == "BSM"


def test_other_message():
    timestamp = "7/22/2020  9:20:28 AM"
    message_type = "warningFCW"
    row = {
        "EventType": message_type,
        "EventData": _warning_message(),
        "Timestamp": timestamp,
    }

    [row] = convert_row([], row)

    assert row["messageBody"]["driverWarn"]
    assert row["timestamp"] == timestamp
    assert row["messageType"] == "warningFCW"


def _bsm():
    return """<WSMP-EventData><dot3><channel>172</channel><psid>20</psid><signal><rxStrength>-30</rxStrength></signal><dataRate>12</dataRate><timeSlot>0</timeSlot></dot3><message><MessageFrame><messageId>20</messageId><value><BasicSafetyMessage><coreData><msgCnt>4</msgCnt><id>090001BC</id><secMark>27392</secMark><lat>265523070</lat><long>-800913555</long><elev>-221</elev><accuracy><semiMajor>7</semiMajor><semiMinor>6</semiMinor><orientation>31050</orientation></accuracy><transmission><unavailable/></transmission><speed>932</speed><heading>16288</heading><angle>127</angle><accelSet><long>31</long><lat>-19</lat><vert>-49</vert><yaw>10</yaw></accelSet><brakes><wheelBrakes>10000</wheelBrakes><traction><unavailable/></traction><abs><unavailable/></abs><scs><unavailable/></scs><brakeBoost><off/></brakeBoost><auxBrakes><unavailable/></auxBrakes></brakes><size><width>180</width><length>450</length></size></coreData><partII><SEQUENCE><partII-Id>0</partII-Id><partII-Value><VehicleSafetyExtensions><pathHistory><crumbData><PathHistoryPoint><latOffset>-14120</latOffset><lonOffset>-6822</lonOffset><elevationOffset>10</elevationOffset><timeOffset>927</timeOffset></PathHistoryPoint><PathHistoryPoint><latOffset>21066</latOffset><lonOffset>10176</lonOffset><elevationOffset>-4</elevationOffset><timeOffset>1801</timeOffset></PathHistoryPoint></crumbData></pathHistory><pathPrediction><radiusOfCurve>32767</radiusOfCurve><confidence>200</confidence></pathPrediction></VehicleSafetyExtensions></partII-Value></SEQUENCE></partII></BasicSafetyMessage></value></MessageFrame></message></WSMP-EventData>"""


def _warning_message():
    return """<WarningEventData><id>683FDDAC</id><driverWarn><true/></driverWarn><isControl><false/></isControl><isDisabled><false/></isDisabled><hvBSM><MessageFrame><messageId>20</messageId><value><BasicSafetyMessage><coreData><msgCnt>62</msgCnt><id>0A00014D</id><secMark>27481</secMark><lat>265524793</lat><long>-800912790</long><elev>-225</elev><accuracy><semiMajor>8</semiMajor><semiMinor>6</semiMinor><orientation>29889</orientation></accuracy><transmission><unavailable/></transmission><speed>1192</speed><heading>16281</heading><angle>127</angle><accelSet><long>-9</long><lat>12</lat><vert>-48</vert><yaw>-2</yaw></accelSet><brakes><wheelBrakes>10000</wheelBrakes><traction><unavailable/></traction><abs><unavailable/></abs><scs><unavailable/></scs><brakeBoost><off/></brakeBoost><auxBrakes><unavailable/></auxBrakes></brakes><size><width>180</width><length>450</length></size></coreData><partII><SEQUENCE><partII-Id>0</partII-Id><partII-Value><VehicleSafetyExtensions><pathHistory><crumbData><PathHistoryPoint><latOffset>-12265</latOffset><lonOffset>-5895</lonOffset><elevationOffset>-8</elevationOffset><timeOffset>633</timeOffset></PathHistoryPoint><PathHistoryPoint><latOffset>23413</latOffset><lonOffset>11237</lonOffset><elevationOffset>-5</elevationOffset><timeOffset>1977</timeOffset></PathHistoryPoint></crumbData></pathHistory><pathPrediction><radiusOfCurve>32767</radiusOfCurve><confidence>200</confidence></pathPrediction></VehicleSafetyExtensions></partII-Value></SEQUENCE></partII></BasicSafetyMessage></value></MessageFrame></hvBSM><rvBSM><MessageFrame><messageId>20</messageId><value><BasicSafetyMessage><coreData><msgCnt>4</msgCnt><id>090001BC</id><secMark>27392</secMark><lat>265523070</lat><long>-800913555</long><elev>-221</elev><accuracy><semiMajor>7</semiMajor><semiMinor>6</semiMinor><orientation>31050</orientation></accuracy><transmission><unavailable/></transmission><speed>932</speed><heading>16288</heading><angle>127</angle><accelSet><long>31</long><lat>-19</lat><vert>-49</vert><yaw>10</yaw></accelSet><brakes><wheelBrakes>10000</wheelBrakes><traction><unavailable/></traction><abs><unavailable/></abs><scs><unavailable/></scs><brakeBoost><off/></brakeBoost><auxBrakes><unavailable/></auxBrakes></brakes><size><width>180</width><length>450</length></size></coreData><partII><SEQUENCE><partII-Id>0</partII-Id><partII-Value><VehicleSafetyExtensions><pathHistory><crumbData><PathHistoryPoint><latOffset>-14120</latOffset><lonOffset>-6822</lonOffset><elevationOffset>10</elevationOffset><timeOffset>927</timeOffset></PathHistoryPoint><PathHistoryPoint><latOffset>21066</latOffset><lonOffset>10176</lonOffset><elevationOffset>-4</elevationOffset><timeOffset>1801</timeOffset></PathHistoryPoint></crumbData></pathHistory><pathPrediction><radiusOfCurve>32767</radiusOfCurve><confidence>200</confidence></pathPrediction></VehicleSafetyExtensions></partII-Value></SEQUENCE></partII></BasicSafetyMessage></value></MessageFrame></rvBSM></WarningEventData>"""
