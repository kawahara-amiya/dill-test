from datetime import datetime, timedelta, time, date
import pandas as pd
from pandas import DataFrame, Series
import numpy as np


class FreqUtil(object):
    u"""docstring for FreqUtil
        頻度計算のためのデータ整理加工。
    """

    def roll_summary(
            self,
            df: DataFrame,
            collected_hour: int,
            end_date: datetime.date
    ) -> DataFrame:
        u"""
            指定された集計表に対して、時間方向に移動平均処理をかけたものを返す。
            Parameters
            ----------
                df : DataFrame
                    カラム : 各日付 ( : date)
                    インデックス: 各時間( : time)

                    |          | 2018-06-02 | 2018-06-03 | ... |
                    | 00:00:00 | 0          | 0          |     |
                    | 01:00:00 | 0          | 2          |     |
                    | ...      |            |            |     |
        """

        day_alltime = [time(hour=hour) for hour in range(0, 24)]
        base_series = Series(dict(zip(day_alltime, [0] * 24)))
        last_indexes = [f"last_{h}" for h in range(0, 24)]
        next_indexes = [f"next_{h}" for h in range(0, 24)]

        # 先日の翌日の分まで移動平均計算に反映させるため、
        # 各カラムに先日と翌日の分のデータをつぎ足した「大きな」dataframeを作る
        big_df = pd.DataFrame(
            index=pd.concat([Series(index=last_indexes), base_series, Series(index=next_indexes)]).index
        )
        big_df = pd.concat([big_df, df], axis=1, sort=False)

        for date_, _ in df.items():
            lastdate = date_ - timedelta(days=1)
            nextdate = date_ + timedelta(days=1)
            if lastdate in df.columns:
                values = df[lastdate].values
                big_df.update(
                    DataFrame(data=values, index=last_indexes, columns=[date_])
                )
            if nextdate in df.columns:
                values = df[nextdate].values
                big_df.update(
                    DataFrame(data=values, index=next_indexes, columns=[date_])
                )

        # windowの値に妥当な理由はない。
        # とりあえず前後3時間分影響が及ぶように7にしている
        # これがある程度大きいことで、特定時刻における大きな異常値が、より周辺時刻へ波及し、デイリースコアに大きな影響を与えられる、というメリットがある
        # その時刻においては同じスコア100で区別不能でも、周辺時刻への影響によって、十分大きな異常値同士もデイリースコアにおいては差別化できる、ということ
        big_df = big_df.rolling(
            window=7, center=True, min_periods=1, win_type="triang").mean()

        # もともとのデータがNaNだったところには-1を代入する(updateで現在の値をNaNに更新できないため)
        # 後でスコアを算出するときに-1ものはNoneに変換する
        if collected_hour is not None:
            values = [-1] * (23 - collected_hour)
            uncollected_time = \
                [time(hour=hour) for hour in range(collected_hour + 1, 24)]
            big_df.update(
                DataFrame(
                    data=values, index=uncollected_time, columns=[end_date]
                )
            )

        # 移動平均計算が終わったので、各カラムが扱うべき日のデータ部分だけに戻す
        return big_df[24:48]
