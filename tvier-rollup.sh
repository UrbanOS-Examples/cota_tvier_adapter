#!/usr/bin/env bash
# This script allows for the historical capture and rollup of multiple days of TVIER data. 
# As of now, these files will be massive and this should be run on a remote server.

function get_for_day {
    day=$1
    curl -o "${day}.json" --keepalive-time 2 --location --request GET "localhost:5000/api/v1/tvier?url=https://www.cota.com/COTA/media/COTAContent/cota_obu_data_${day}.gz"
}

d=2021_01_01
while [ "$d" != 2021_01_31 ]; do 
  echo $d
  get_for_day $d
  d=$(date -j -v +1d -f "%Y_%m_%d" "${d}" +%Y_%m_%d)
done

jq -s -c "." 2021*.json > rollup.json