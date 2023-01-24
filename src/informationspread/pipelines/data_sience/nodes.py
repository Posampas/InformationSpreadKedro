import pandas as pd


def remove_non_polish_locations(input: pd.DataFrame) -> pd.DataFrame:
    return input[input["country"] == "Polska"]


def db_scan_node(input: pd.DataFrame):
    print(len(input))


["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "feature class""feature code", "country code", "cc2",
    "admin1 code",  "admin2 code", "admin3 code",  "admin4 code", "population",   "elevation", "dem", "timezone", "modification date"]
