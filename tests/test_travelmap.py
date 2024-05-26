import polars as pl

from travelmap.travelmap import load_cities, get_station_info

def test_load_cities():
    cities = load_cities()
    assert len(cities) == 711

def test_get_station_info():
    station_info = get_station_info("Lindau-Reutin")
    print(station_info)
    assert station_info["extId"] == "8003693"
    assert station_info["id"] == "A=1@O=Lindau-Reutin@X=9703289@Y=47552388@U=80@L=8003693@B=1@p=1715799947@i=U×008016563@"
    assert station_info["lat"] == 47.552406
    assert station_info["lon"] == 9.70284
    assert station_info["name"] == "Lindau-Reutin"

