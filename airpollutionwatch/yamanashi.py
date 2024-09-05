import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime
from logging import getLogger, basicConfig, INFO, DEBUG
import requests_cache
import pandas as pd
import numpy as np


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
    from airpollutionwatch.convert import (
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
aliases = {
    "吉田": "吉田",  # 長野にもある
    "甲府穴切": "甲府穴切",  # not found
    "移動局_身延": "移動局_身延",  # not found
}

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
    "WS": lambda x: WS(x, unit="m/s"),
    # "TEMP": lambda x: TEMP(x, unit="celsius"),
    # "HUM": lambda x: HUM(x, unit="%"),
}


def stations():
    """独自の測定局コードと測定局名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(STATIONS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://taiki.pref.yamanashi.jp/data/V501Station.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def items():
    """独自の測定量コードと測定量名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(ITEMS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://taiki.pref.yamanashi.jp/data/V502Item.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    logger = getLogger()
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("%Y%m%d%H")
    if date_time[-2:] == "00":
        logger.debug(f"Date spec {date_time} is invalid.")
        # 00時は存在しないので、前日の24時に書きかえる。
        date_time = (dt - datetime.timedelta(hours=1)).strftime("%Y%m%d") + "24"
        logger.debug(f"Modified to {date_time}.")

    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://taiki.pref.yamanashi.jp/data/{date_time[:6]}/{date_time}.json",
    )
    # # これがないと文字化けする
    # response.encoding = response.apparent_encoding
    # with open("tmp.txt", "w") as f:
    #     f.write(response.text)
    # with open("tmp.txt", "r") as f:
    #     text = f.read()

    dfs = pd.read_json(io.StringIO(response.text)).transpose()
    return dfs


def retrieve(isotime, station_set="full"):
    """指定された日時のデータを入手する。index名とcolumn名をつけなおし、単位をそらまめにあわせる。"""
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
    df = pd.concat(cols, axis=1).set_index("station")

    if station_set == "air":
        # station_mapに含まれる測定局のみに絞る
        # selection = [isinstance(i, np.int64) for i in df.index]
        selection = [type(i) != str and (10000000 <= i <= 99999999) for i in df.index]
        df = df.iloc[selection]

    return df


def test():
    print(retrieve("2024-08-08T23:00+09:00"))


if __name__ == "__main__":
    basicConfig(level=DEBUG)
    test()
