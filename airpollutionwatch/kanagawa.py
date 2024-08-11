import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime

# import requests
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
    "逗子市逗子": "逗子市逗子",  # not found
    "西丹沢犬越路": "西丹沢犬越路",  # not found
    "移動局山北町": "移動局山北町",  # not found
    "国設川崎（田島）": "国設川崎",
    "中原区地域みまもり支援センター": "中原みまもりＣ",
    "高津区生活文化会館": "生活文化会館",
    "多摩区登戸小学校": "登戸小学校",
    "麻生区弘法松公園": "弘法松公園",
    "川崎区池上新田公園前": "池上新田公園前",
    "川崎区日進町": "日進町",
    "幸区遠藤町交差点": "遠藤町交差点",
    "高津区二子": "二子",
    "多摩区本村橋": "本村橋",
    "麻生区柿生": "柿生",
    "川崎区富士見公園": "富士見公園",
    "川崎市役所第３庁舎": "川崎市役所第３庁舎",  # not found
    "横須賀市追浜行政センター": "追浜行政Ｃ",
    "横須賀市久里浜行政センター": "久里浜行政Ｃ",
    "横須賀市西行政センター": "西行政Ｃ",
    "横須賀市小川町交差点": "小川町交差点",
    "横須賀市池上ｺﾐｭﾆﾃｨｾﾝﾀｰ": "池上コミＣ",
    "相模原市相模台": "相模台",
    "相模原市橋本": "橋本",
    "相模原市田名": "田名",
    "相模原市津久井": "津久井",
    "相模原市上溝": "上溝",
    "相模原市古淵": "古淵",
    "藤沢市湘南台小学校": "湘南台小学校",
    "藤沢市御所見小学校": "御所見小学校",
    "藤沢市明治市民センター": "明治市民センター",
    "平塚市大野公民館": "大野公民館",
    "平塚市神田小学校": "神田小学校",
    "平塚市旭小学校": "旭小学校",
    "平塚市花水小学校": "花水小学校",
    "平塚市松原歩道橋": "松原歩道橋",
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
    "WV": lambda x: WS(x, unit="m/s"),
    "TEMP": lambda x: TEMP(x, unit="celsius"),
    "HUM": lambda x: HUM(x, unit="%"),
}


def stations():
    """独自の測定局コードと測定局名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(STATIONS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.pref.kanagawa.jp/sys/taikikanshi/kanshi/data/V501Station.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def items():
    """独自の測定量コードと測定量名の関係を定義するファイルを入手する。"""
    # dfs = pd.DataFrame.from_dict(ITEMS, orient="index")  # .transpose()
    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        "https://www.pref.kanagawa.jp/sys/taikikanshi/kanshi/data/V502Item.json",
    )
    dfs = pd.read_json(io.StringIO(response.text), orient="index")  # .transpose()
    return dfs


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
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
    print(retrieve("2024-08-08T23:00+09:00"))


if __name__ == "__main__":
    test()
