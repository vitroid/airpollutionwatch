import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime
from logging import getLogger, basicConfig, INFO, DEBUG
import requests_cache
import pandas as pd
import numpy as np

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
    logger = getLogger()
    dt = datetime.datetime.fromisoformat(isotime)
    date_time = dt.strftime("day=%Y年%m月%d日&hour=%H")
    if date_time[-2:] == "00":
        logger.debug(f"Date spec {date_time} is invalid.")
        # 00時は存在しないので、前日の24時に書きかえる。
        date_time = (dt - datetime.timedelta(hours=1)).strftime(
            "day=%Y年%m月%d日&hour="
        ) + "24"
        logger.debug(f"Modified to {date_time}.")

    session = requests_cache.CachedSession("airpollution")
    response = session.get(
        f"https://air.taiki.pref.chiba.lg.jp/hourreport/?{date_time}",
    )
    # これがないと文字化けする
    response.encoding = response.apparent_encoding

    logger.debug(response.text)
    dfs = pd.read_html(io.StringIO(response.text))
    return dfs[0]


def retrieve(isotime, station_set="full"):
    """指定された日時のデータを入手する。index名とcolumn名をつけなおし、単位をそらまめにあわせる。"""
    logger = getLogger()

    df = retrieve_raw(isotime)
    # with open("tmp.pickle", "wb") as f:
    #     pickle.dump(df, f)
    # with open("tmp.pickle", "rb") as f:
    #     df = pickle.load(f)

    cols = []
    for col in df.columns:
        if col in converters:
            cols.append(converters[col](df[col]))
    df = pd.concat(cols, axis=1).set_index("station")

    if station_set == "air":
        # station_mapに含まれる測定局のみに絞る
        selection = [type(i) != str and (10000000 <= i <= 99999999) for i in df.index]
        df = df.iloc[selection]

    return df


def test():
    print(retrieve("2024-09-01T00:00+09:00"))


if __name__ == "__main__":
    basicConfig(level=DEBUG)
    test()


# 千葉のシステムは、間違った日付文字列を与えると(あるいは、それ以外のエラーでも)今日の最新データをしれっと表示するのでとても怖い。
# 以下の部分をBeautifulSoupあたりでチェックし、日付と時刻がちゃんと選択されていることを確認したほうが良い。
"""
<main>
    <div id="contents">
        <h1>リアルタイム時報<span>県内の全測定局の全測定物質の1時間値を一覧表で見ることができます。</span></h1>
        <div id="search">
            <form method="get" action="./">
                <input type="text" value="2024年08月31日" name="day" id="day" readonly="readonly">
                <select name="hour" id="hour">
                    <option value="01">01</option><option value="02">02</option><option value="03">03</option><option value="04">04</option><option value="05">05</option><option value="06">06</option><option value="07">07</option><option value="08">08</option><option value="09">09</option><option value="10">10</option><option value="11">11</option><option value="12">12</option><option value="13">13</option><option value="14">14</option><option value="15">15</option><option value="16">16</option><option value="17">17</option><option value="18">18</option><option value="19">19</option><option value="20">20</option><option value="21">21</option><option value="22">22</option><option value="23">23</option><option value="24" selected="selected">24</option>                              </select>
"""
