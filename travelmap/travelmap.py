from pathlib import Path
from time import sleep
from random import randint
import argparse

import polars as pl
import hvplot.polars  # noqa
import httpx
import xyzservices.providers as xyz


WAITING_TIME_SECONDS_STATION_INFO = 2
CITY_PATH = Path(__file__).parent.parent / Path("German-cities.xlsx")
STATION_INFO_OUTPUT_PARQUET = Path(__file__).parent.parent / Path("station-info.parquet")
STATION_INFO_OUTPUT_TSV = Path(__file__).parent.parent / Path("station-info.tsv")
GRAPH_OUTPUT_PATH = Path(__file__).parent.parent / Path("station-info-graph.png")

URL_STATION = "https://www.bahn.de/web/api/reiseloesung/orte"
COOKIES_STATION = {}
HEADERS_STATION = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Accept": "application/json",
    "Accept-Language": "de",
    "x-correlation-id": "6b47ec5c-b18e-4a22-b397-36b555f2455c_b7e3c565-98e5-4368-a0b3-606f4b9d237a",
    "Connection": "keep-alive",
    "Referer": "https://www.bahn.de/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-GPC": "1",
}
PARAMS_STATION = {
    "suchbegriff": "<insert here>",
    "typ": "ALL",
    "limit": "10",
}


def get_cli_arguments():
    parser = argparse.ArgumentParser(description="Produce a travel map.")
    parser.add_argument("--refresh_station_info", default=False, action="store_true")
    return parser.parse_args()


def load_cities(path: Path | str = CITY_PATH) -> pl.DataFrame:
    cities = pl.read_excel(source=path, engine="calamine")
    return cities


def get_station_info(station_name: str, 
                     url: str = URL_STATION,
                     cookies: dict[str] = COOKIES_STATION, 
                     headers: dict[str] = HEADERS_STATION, 
                     params_template: dict[str] = PARAMS_STATION) -> dict:
    params = params_template.copy()
    params["suchbegriff"] = station_name
    response = httpx.get(url, params=params, cookies=cookies, headers=headers)
    return response.json()[0]  # contains fields extId (not needed), id, lat, lon, name


def city2station(city_name: str) -> str:
    if city_name in ("Neu-Ulm", "Bocholt", "Hameln", "Hürth", "Bergheim", "Rheine", "Marl"):
        return city_name
    if city_name == "Heidenheim an der Brenz":
        return "Rüsselsheim"    
    if city_name == "Heidenheim":
        return "Rüsselsheim"
    if city_name == "Bad Homburg vor der Höhe":
        return "Bad Homburg"
    if city_name == "Neustadt an der Weinstraße":
        return "Neustadt(Weinstr)Hbf"
    return city_name + " Hbf"


def reformat_station_info(station_info: dict, city_name: str) -> list:
    keys = ["extId", "id", "lat", "lon", "name"]
    return [city_name] + [station_info[key] for key in keys]


def save_graph(data: pl.DataFrame, path: Path | str):
    hvplot.extension("matplotlib")
    plot = data.hvplot.points(
                                x="lon",
                                y="lat",
                                geo=True,
                                tiles="EsriImagery",
                                alpha=0.7,
                                width=5000,
                             )
    hvplot.save(plot, path)


if __name__ == "__main__":
    args = get_cli_arguments()
    cities = load_cities()
    cities = cities.filter(pl.col("Rang") <= "201.")  # we only want sufficiently large cities 201

    if args.refresh_station_info:
        city_names = cities["Name"].to_list()
        print("Getting city details, with some wait between the requests")
        station_info_rows = []
        for city_name in city_names:
            station_info = get_station_info(city2station(city_name))
            station_info_rows.append(reformat_station_info(station_info=station_info, city_name=city_name))
            sleep(WAITING_TIME_SECONDS_STATION_INFO + randint(0, WAITING_TIME_SECONDS_STATION_INFO/2) - WAITING_TIME_SECONDS_STATION_INFO/2)
            print(station_info_rows[-1])
        station_info_df = pl.DataFrame(data=station_info_rows, schema={"original_name": pl.String, 
                                                                       "extId": pl.String, 
                                                                       "id": pl.String, 
                                                                       "lat": pl.Float32, 
                                                                       "lon": pl.Float32, 
                                                                       "name": pl.String}, 
                                                                orient="row"
                                        )
        station_info_df.write_csv(STATION_INFO_OUTPUT_TSV, separator="\t")
        station_info_df.write_parquet(STATION_INFO_OUTPUT_PARQUET)
    else:
        station_info_df = pl.read_parquet(STATION_INFO_OUTPUT_PARQUET)
    
    save_graph(data=station_info_df, path=GRAPH_OUTPUT_PATH)

    
     
