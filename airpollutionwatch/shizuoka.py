import sys

sys.path.insert(0, "..")  # for debug

import io
import datetime

# import requests
import requests_cache
import pandas as pd

try:
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
except:
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


# ウェブ上の表記と、国環研の表記との対応
aliases = {
    "下田総合庁舎": "下田総合庁舎",  # not found
    "蒲原測定局": "蒲原",
    "三保第一小学校": "清水三保第一小",
    "庵原中学校": "清水庵原中学校",
    "興津北公園": "清水興津北公園",
    "中央": "浜松中央測定局",
    "東南部": "東南部測定局",
    "西部": "西部測定局",
    "東北部": "東北部測定局",
    "三ヶ日": "三ヶ日測定局",
    "天竜": "天竜測定局",
    "Ｒ２５７": "Ｒ－２５７",
    "Ｒ１５０": "Ｒ－１５０",
    "浜松環状線測定局": "浜松環状線",
    "浜北": "浜北測定局",
    "引佐": "引佐測定局",
}

# apparent nameと内部標準名(そらまめ名)の変換
converters = {
    # "地域",
    "測定局名": lambda x: STATION(x, aliases=aliases),
    # "測定局",
    # "種別",
    "二酸化硫黄SO2(ppm)": lambda x: SO2(x, unit="ppm"),
    "一酸化窒素NO(ppm)": lambda x: NO(x, unit="ppm"),
    "二酸化窒素NO2(ppm)": lambda x: NO2(x, unit="ppm"),
    "窒素酸化物NOX(ppm)": lambda x: NOX(x, unit="ppm"),
    "光化学オキ シダントOX(ppm)": lambda x: OX(x, unit="ppm"),
    "浮遊粒子状 物質SPM(mg/m3)": lambda x: SPM(x, unit="mg/m3"),
    "微小粒子状 物質PM2.5(μg/m3)": lambda x: PM25(x, unit="ug/m3"),
    "非メタン 炭化水素NMHC(ppmC)": lambda x: NMHC(x, unit="ppmC"),
    "メタンCH4(ppmC)": lambda x: CH4(x, unit="ppmC"),
    "全炭化水素THC(ppmC)": lambda x: THC(x, unit="ppmC"),
    "一酸化炭素CO(ppm)": lambda x: CO(x, unit="ppm"),
    # "WD 方位": lambda x: WD(x, unit="EN"),
    # "WV m/s": lambda x: WS(x, unit="m/s"),
    # "TEMP ℃": lambda x: TEMP(x, unit="celsius"),
    # "HUM %": lambda x: HUM(x, unit="%"),
}


def retrieve_raw(isotime):
    """指定された日時のデータを入手する。index名とcolumn名は生のまま。"""
    dt = datetime.datetime.fromisoformat(isotime)
    date = dt.strftime("%Y%m%d")
    time = dt.strftime("%H")

    data = {
        "time": time,
        "date": date,
        "operation": "non",
        # "_token": "XYZ468SEcJMjhXEW4CNgmcadv7D7w7JlpSzQBhzu"
    }
    session = requests_cache.CachedSession("airpollution")
    response = session.post(
        "https://taikikanshi.pref.shizuoka.jp/jiho",
        data=data,
    )
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
