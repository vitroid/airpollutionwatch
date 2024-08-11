import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime

# import requests
import requests_cache
import pandas as pd

# try:
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

# except:
#     from convert import (
#         SO2,
#         NO,
#         NO2,
#         NOX,
#         OX,
#         SPM,
#         PM25,
#         NMHC,
#         CH4,
#         THC,
#         CO,
#         WD,
#         WS,
#         TEMP,
#         HUM,
#         STATION,
#     )


# ウェブ上の表記と、国環研の表記との対応
aliases = {
    "野田宮崎": "国設野田",
    "柏旭": "柏旭（車）",
    "柏西原": "柏西原（車）",
    "柏大津ケ丘": "柏大津ケ丘（車）",
    "松戸上本郷": "松戸上本郷（車）",
    "市川市市川": "市川市市川（車）",
    "市川行徳": "市川行徳（車）",
    "市川稲荷木": "市川稲荷木",
    "浦安美浜": "浦安美浜（車）",
    "船橋海神": "船橋海神（車）",
    "船橋日の出": "船橋日の出（車）",
    "八千代村上": "八千代村上（車）",
    "(千葉)花見川小学校": "花見川小学校",
    "(千葉)検見川小学校": "検見川小学校",
    "(千葉)山王小学校": "山王小学校",
    "(千葉)宮野木": "宮野木",
    "(千葉)大宮小学校": "大宮小学校",
    "(千葉)千城台わかば小学校": "千城台わかば小学校",
    "(千葉)泉谷小学校": "泉谷小学校",
    "(千葉)寒川小学校": "寒川小学校",
    "(千葉)福正寺": "福正寺",
    "(千葉)蘇我保育所": "蘇我保育所",
    "(千葉)都公園": "都公園",
    "(千葉)土気": "土気",
    "(千葉)真砂公園": "真砂公園",
    "(千葉)千草自排": "千草自排",
    "(千葉)葭川自排": "葭川自排",
    "(千葉)宮野木自排": "宮野木自排",
    "(千葉)真砂自排": "真砂自排",
    "佐倉山王": "佐倉山王（車）",
    "市原中川田": "市原中川田（車）",
    "袖ケ浦三ツ作": "袖ケ浦三ッ作",
    "袖ケ浦福王台": "袖ケ浦福王台",
    "袖ケ浦大曽根": "袖ケ浦大曽根",
    "木更津請西": "木更津請西（車）",
    "木更津牛袋": "木更津牛袋（車）",
    "成田花崎": "成田花崎（車）",
}

# apparent nameと内部標準名(そらまめ名)の変換
converters = {
    # "地域",
    "測定局": lambda x: STATION(x, aliases=aliases),
    # "測定局",
    # "種別",
    "SO2 ppm": lambda x: SO2(x, unit="ppm"),
    "NO ppm": lambda x: NO(x, unit="ppm"),
    "NO2 ppm": lambda x: NO2(x, unit="ppm"),
    "NOX ppm": lambda x: NOX(x, unit="ppm"),
    "OX ppm": lambda x: OX(x, unit="ppm"),
    "SPM mg/m3": lambda x: SPM(x, unit="mg/m3"),
    "PM2.5 ug/m3": lambda x: PM25(x, unit="ug/m3"),
    "NMHC ppmC": lambda x: NMHC(x, unit="ppmC"),
    "CH4 ppmC": lambda x: CH4(x, unit="ppmC"),
    "THC ppmC": lambda x: THC(x, unit="ppmC"),
    "CO ppm": lambda x: CO(x, unit="ppm"),
    "WD 方位": lambda x: WD(x, unit="EN"),
    "WV m/s": lambda x: WS(x, unit="m/s"),
    "TEMP ℃": lambda x: TEMP(x, unit="celsius"),
    "HUM %": lambda x: HUM(x, unit="%"),
}


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("day=%Y年%m月%d日&hour=%H")

    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://air.taiki.pref.chiba.lg.jp/hourreport/?{date_time}",
    )
    # これがないと文字化けする
    response.encoding = response.apparent_encoding

    dfs = pd.read_html(io.StringIO(response.text))
    return dfs[0]


def retrieve(isotime):
    """指定された日時のデータを入手する。index名とcolumn名をつけなおし、単位をそらまめにあわせる。"""
    df = retrieve_raw(isotime)
    # with open("tmp.pickle", "wb") as f:
    #     pickle.dump(df, f)
    # with open("tmp.pickle", "rb") as f:
    #     df = pickle.load(f)

    cols = []
    for col in df.columns:
        if col in converters:
            cols.append(converters[col](df[col]))
    return pd.concat(cols, axis=1).set_index("station")


def test():
    print(retrieve("2024-08-08T23:00+09:00"))


if __name__ == "__main__":
    test()
