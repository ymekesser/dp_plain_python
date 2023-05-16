import logging
import requests

log = logging.getLogger(__name__)

# todo: Make configurable
overpass_endpoint = "https://overpass-api.de/api/interpreter"

shopping_mall_query = """
[out:json];
area["ISO3166-1"="SG"][admin_level=2]->.sg;
(
  node["shop"="mall"](area.sg);
  way["shop"="mall"](area.sg);
  relation["shop"="mall"](area.sg);
);
out center;
"""

mrt_query = """
[out:json];
area["ISO3166-1"="SG"][admin_level=2];
node(area)["subway"="yes"];
out;
"""


def get_shopping_malls_geodata():
    response = _get(shopping_mall_query)

    return response


def get_mrt_stations_geodata():
    response = _get(mrt_query)

    return response


def _get(query):
    log.info(f"Querying overpass ({overpass_endpoint}). Query: {query}")
    response = requests.get(overpass_endpoint, params={"data": query})

    if response.status_code != 200:
        error_msg = (
            f"Overpass API returned {response.status_code} status code: {response.text}"
        )
        log.exception(error_msg)
        raise requests.HTTPError(error_msg)

    log.info(f"Overpass query successful")

    return response.json()
