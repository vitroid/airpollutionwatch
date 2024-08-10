import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime

# import requests
import requests_cache
import pandas as pd

# from airpollution.kanagawa_aux.V501Station import STATIONS
# from airpollution.kanagawa_aux.V502Item import ITEMS

try:
    from convert import (
        SO2,
        NO,
        NO2,
        NOX,
        OX,
        SPM,
        PM25,
        NMHC,
        CH4,
        THC,
        CO,
        WD,
        WS,
        TEMP,
        HUM,
        STATION,
    )
except:
    from airpollution.convert import (
        SO2,
        NO,
        NO2,
        NOX,
        OX,
        SPM,
        PM25,
        NMHC,
        CH4,
        THC,
        CO,
        WD,
        WS,
        TEMP,
        HUM,
        STATION,
    )


# ウェブ上の表記と、国環研の表記との対応
aliases = {}

# apparent nameと内部標準名(そらまめ名)の変換
converters = {
    # "地域",
    "name": lambda x: STATION(x, aliases=aliases),
    # "測定局",
    # "種別",
    "SO2": lambda x: SO2(x, unit="ppm"),
    "NO": lambda x: NO(x, unit="ppm"),
    "NO2": lambda x: NO2(x, unit="ppm"),
    "NOX": lambda x: NOX(x, unit="ppm"),
    "OX": lambda x: OX(x, unit="ppm"),
    "SPM": lambda x: SPM(x, unit="mg/m3"),
    "PM2.5": lambda x: PM25(x, unit="ug/m3"),
    "NMHC": lambda x: NMHC(x, unit="ppmC"),
    "CH4": lambda x: CH4(x, unit="ppmC"),
    "THC": lambda x: THC(x, unit="ppmC"),
    "CO": lambda x: CO(x, unit="ppm"),
    "WD": lambda x: WD(x, unit="16dirc"),  # integer
    "WV": lambda x: WS(x, unit="m/s"),
    "TEMP": lambda x: TEMP(x, unit="celsius"),
    "HUM": lambda x: HUM(x, unit="%"),
}


def stations():
    # dfs = pd.DataFrame.from_dict(STATIONS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.pref.kanagawa.jp/sys/taikikanshi/kanshi/data/V501Station.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def items():
    # dfs = pd.DataFrame.from_dict(ITEMS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.pref.kanagawa.jp/sys/taikikanshi/kanshi/data/V502Item.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def retrieve_raw(isotime):
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("%Y%m%d%H")

    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://www.pref.kanagawa.jp/sys/taikikanshi/kanshi/data/{date_time[:6]}/{date_time}.json",
    )
    # # これがないと文字化けする
    # response.encoding = response.apparent_encoding
    # with open("tmp.txt", "w") as f:
    #     f.write(response.text)
    # with open("tmp.txt", "r") as f:
    #     text = f.read()

    dfs = pd.read_json(io.StringIO(response.text)).transpose()
    return dfs


def retrieve(isotime):
    df = retrieve_raw(isotime)
    item_map = items()["simpleName"].to_dict()
    # データをpyから読む場合は、codeが整数化されてしまう。
    item_map = {int(x): y for x, y in item_map.items()}

    station_map = stations()["name"].to_dict()
    # データをpyから読む場合は、codeが整数化されてしまう。
    station_map = {int(x): y for x, y in station_map.items()}

    df = df.rename(index=station_map, columns=item_map)
    df["name"] = df.index

    cols = []
    for col in df.columns:
        if col in converters:
            cols.append(converters[col](df[col]))
    return pd.concat(cols, axis=1).set_index("station")


def test():
    print(retrieve("2024-08-08T23:00+09:00"))


if __name__ == "__main__":
    test()
