import pandas as pd
from airpollutionwatch.TM20210000 import STATIONS
import io

# with open("TM20210000.txt") as f:
#     stations = pd.read_csv(f)
stations = pd.read_csv(io.StringIO(STATIONS))
stations = stations[
    [
        "測定局名",
        "８文字名",
        "国環研局番",
        "経度_度",
        "経度_分",
        "経度_秒",
        "緯度_度",
        "緯度_分",
        "緯度_秒",
        "標高(m)",
    ]
]
stations["経度"] = (
    stations["経度_度"] + stations["経度_分"] / 60 + stations["経度_秒"] / 3600
)
stations["緯度"] = (
    stations["緯度_度"] + stations["緯度_分"] / 60 + stations["緯度_秒"] / 3600
)
stations = stations[
    [
        "測定局名",
        "８文字名",
        "国環研局番",
        "経度",
        "緯度",
        "標高(m)",
    ]
]
stations = stations.set_index("国環研局番")
# print(stations.loc["野田桐ケ作"])


def station_to_id(station, aliases=None):
    if station in aliases:
        station = aliases[station]
    match1 = stations["測定局名"] == station
    match2 = stations["８文字名"] == station
    rows = stations[match1 | match2]
    if len(rows) == 1:
        return rows.index[0]
    print(station)
    return station


# converters
# そらまめの生データにあわせる
def PPB(series: pd.Series, unit: str = "ppb"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "ppm":
        return series * 1000
    assert unit == "ppb", f"unknown unit {unit}"
    return series


def dPPM(series: pd.Series, unit: str = "0.1ppm"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "ppm":
        return series * 10
    assert unit == "0.1ppm", f"unknown unit {unit}"
    return series


def UG_M3(series: pd.Series, unit: str = "ug/m3"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "mg/m3":
        return series * 1000
    assert unit == "ug/m3", f"unknown unit {unit}"
    return series


def dM_S(series: pd.Series, unit: str = "0.1m/s"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "m/s":
        return series * 10
    assert unit == "0.1m/s", f"unknown unit {unit}"
    return series


def DPPBC(series: pd.Series, unit: str = "10ppbC"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "ppbC":
        return series / 10
    elif unit == "ppmC":
        return series * 100

    assert unit == "10ppbC", f"unknown unit {unit}"
    return series


def CELSIUS(series: pd.Series, unit: str = "0.1celsius"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "celsius":
        return series * 10
    assert unit == "0.1celsius", f"unknown unit {unit}"
    return series


def DEGREE(series: pd.Series, unit: str = "degree"):
    series = pd.to_numeric(series, errors="coerce")
    assert unit == "degree", f"unknown unit {unit}"
    return series


def NOP(series: pd.Series):
    series = pd.to_numeric(series, errors="coerce")
    return series


def PERCENT(series: pd.Series, unit: str = "%"):
    series = pd.to_numeric(series, errors="coerce")
    if unit == "0.1%":
        return series / 10
    assert unit == "%", f"unknown unit {unit}"
    return series


def STATION(series: pd.Series, aliases: dict):
    return series.apply(lambda x: station_to_id(x, aliases)).rename("station")


wd_codes_EN = {
    x: i
    for i, x in enumerate(
        [
            "",
            "NNE",
            "NE",
            "ENE",
            "E",
            "ESE",
            "SE",
            "SSE",
            "S",
            "SSW",
            "SW",
            "WSW",
            "W",
            "WNW",
            "NW",
            "NNW",
            "N",
            "CALM",
        ]
    )
}


def DIRC16(series: pd.Series, unit: str = "16dirc"):
    if unit == "EN":
        # 英文字表示を気象庁の16方位数に換算する。
        return series.apply(lambda x: wd_codes_EN.get(x, 0)).astype("Int64")
    assert unit == "16dirc", f"unknown unit {unit}"
    return series.apply(lambda x: pd.to_numeric(x, errors="coerce")).astype("Int64")


def SO2(series: pd.Series, unit: str = "ppb"):
    return PPB(series, unit).rename("SO2")


def NO(series: pd.Series, unit: str = "ppb"):
    return PPB(series, unit).rename("NO")


def NO2(series: pd.Series, unit: str = "ppb"):
    return PPB(series, unit).rename("NO2")


def NOX(series: pd.Series, unit: str = "ppb"):
    return PPB(series, unit).rename("NOX")


def OX(series: pd.Series, unit: str = "ppb"):
    return PPB(series, unit).rename("OX")


def CO(series: pd.Series, unit: str = "0.1ppm"):
    return dPPM(series, unit).rename("CO")


def NMHC(series: pd.Series, unit: str = "10ppbC"):
    return DPPBC(series, unit).rename("NMHC")


def CH4(series: pd.Series, unit: str = "10ppbC"):
    return DPPBC(series, unit).rename("CH4")


def THC(series: pd.Series, unit: str = "10ppbC"):
    return DPPBC(series, unit).rename("THC")


def WD(series: pd.Series, unit: str = "16dirc"):
    return DIRC16(series, unit).rename("WD")


def WS(series: pd.Series, unit: str = "0.1m/s"):
    return dM_S(series, unit).rename("WS")


def TEMP(series: pd.Series, unit: str = "celsius"):
    return CELSIUS(series, unit).rename("TEMP")


def HUM(series: pd.Series, unit: str = "%"):
    return PERCENT(series, unit).rename("HUM")


def SPM(series: pd.Series, unit: str = "ug/m3"):
    return UG_M3(series, unit).rename("SPM")


def PM25(series: pd.Series, unit: str = "ug/m3"):
    return UG_M3(series, unit).rename("PM25")


def LON(series: pd.Series, unit: str = "degree"):
    return DEGREE(series, unit).rename("lon")


def LAT(series: pd.Series, unit: str = "degree"):
    return DEGREE(series, unit).rename("lat")


def CODE(series: pd.Series):
    return NOP(series).rename("code")


def test():
    print(stations.head())


if __name__ == "__main__":
    test()
