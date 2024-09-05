import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime
from logging import getLogger, basicConfig, INFO, DEBUG
import requests_cache
import pandas as pd

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
    "国設東京新宿": "国設東京（新宿）",
    "小金井市東町": "小金井市東町",
    "町田市能ｹ谷": "町田市能ケ谷",
    "八王子市片倉町": "片倉町",
    "八王子市館町": "館町",
    "八王子市大楽寺町": "大楽寺町",
    "玉川通り上馬": "玉川通り上馬",
    "第一京浜高輪": "第一京浜高輪",
    "甲州街道八木町": "八木町",
}

# apparent nameと内部標準名(そらまめ名)の変換
converters = {
    # "地域",
    "name": lambda x: STATION(x, aliases=aliases),
    # "測定局",
    # "種別",
    "SO2": lambda x: SO2(x, unit="ppb"),
    "NO": lambda x: NO(x, unit="ppb"),
    "NO2": lambda x: NO2(x, unit="ppb"),
    "NOX": lambda x: NOX(x, unit="ppb"),
    "OX": lambda x: OX(x, unit="ppb"),
    "SPM": lambda x: SPM(x, unit="ug/m3"),
    "PM2.5": lambda x: PM25(x, unit="ug/m3"),
    "NMHC": lambda x: NMHC(x, unit="10ppbC"),  # ?
    "CH4": lambda x: CH4(x, unit="10ppbC"),  # ?
    "THC": lambda x: THC(x, unit="ppmC"),
    "CO": lambda x: CO(x, unit="0.1ppm"),
    "風向": lambda x: WD(x, unit="16dirc"),  # integer
    "風速": lambda x: WS(x, unit="0.1m/s"),
    "気温": lambda x: TEMP(x, unit="0.1celsius"),
    "湿度": lambda x: HUM(x, unit="0.1%"),
}


def stations():
    """独自の測定局コードと測定局名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(STATIONS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.taiki.kankyo.metro.tokyo.lg.jp/taikikankyo/data/V501Station.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def items():
    """独自の測定量コードと測定量名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(ITEMS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.taiki.kankyo.metro.tokyo.lg.jp/taikikankyo/data/V502Item.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def retrieve_raw(isotime):
    # dt = datetime.datetime.fromisoformat(isotime)
    # date_time = dt.strftime("%Y%m%d%H")
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
        f"https://www.taiki.kankyo.metro.tokyo.lg.jp/taikikankyo/data/hour/{date_time[:6]}/{date_time}.json",
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
    return pd.concat(cols, axis=1).set_index("station")


def test():
    print(retrieve("2024-09-01T00:00+09:00"))


if __name__ == "__main__":
    basicConfig(level=DEBUG)
    test()
