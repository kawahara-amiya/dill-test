from Modules.detector.base import BaseDetector
from Modules.detector.fpdmodel import FPDModel
from Modules.detector.feedback_model import FeedbackModel
from Modules.datasource_container import IDataSourceContainer
import logging
from typing import List
import dill
import re


class FpdDetector(BaseDetector):
    u"""
        ファイルパスの階層構造を考慮した異常検知器
    """

    def __init__(
        self,
        score_rate,
        cutoff_rate,
        split_char_list=None,
        sampling_path_num=1000000,
        fbmodel: FeedbackModel = None
    ):
        u"""
            Parameters
            ----------
                score_rate : int
                    FPDスコアを決定するための指数パラメータ
                    生スコアには影響を与えないが、指数化スコアに用いるため閾値に影響を与えるため
                    閾値調整の際に用いる
                cutoff_rate : float
                    基準パス抽出のパラメータ
                    基準パス抽出におけるアクセスログのディレクトリ出現回数の下限値を決定する
                    デフォルトでは全体のログ数の1%の数値を下限値とする
                split_char_list : パスの分割に使用する区切り文字のリスト
                sampling_path_num : default 1000000
                    基準パス抽出のパラメータ
                    アクセスログが多すぎる場合に、この数値分だけサンプリングを行なってから
                    抽出することで高速化を図る
                    抽出時にサンプリングを行うかどうかの閾値にもなっている
                fbmodel : FeedbackModel
                    検知の時に、このfbmodelのwhitelistフィールドにあるパスに対しては、生スコアを修正する

        """

        self.fpd = FPDModel(
            score_rate=score_rate,
            split_char_list=split_char_list
            )
        self.fbmodel = fbmodel
        self.cutoff_rate = cutoff_rate
        self.sampling_path_num = sampling_path_num

    def learn(
        self,
        datalist: List[str]
    ):
        u"""
            基準パスの抽出と、閾値の決定を行う

            Parameters
            ----------
            datalist : list of str
                基準パス抽出に用いる文字列データリスト
        """
        few_data_exception = FewDataException()
        learning_exception = LearningException()

        # 100件以上学習するログがない場合はスキップする
        if len(datalist) < 100:
            raise few_data_exception

        # 学習するアクセスログ数がsample_path_numより多い場合、サンプリングを用いた基準パス抽出を用いる
        if len(datalist) < self.sampling_path_num:
            # 基準パス抽出時の閾値は全体のパス数の1%とする
            # ただし、最低でも2になるようにする(cutoff < 2の場合、全てのパスが基準パスとして抽出されてしまうため)
            cutoff = round(len(datalist) * self.cutoff_rate)
            if cutoff < 2:
                cutoff = 2
            self.fpd.cutoff = cutoff
            self.fpd.set_frequent_paths(datalist)
        else:
            self.fpd.cutoff = round(self.sampling_path_num * self.cutoff_rate)
            self.fpd.set_frequent_paths(datalist, self.sampling_path_num)

        if self.fpd.frequent_paths is None:
            logging.debug("Frequent path is not extracted")
            raise learning_exception
        else:
            self.fpd.set_Threshold_fpd(datalist)

        if self.fpd.Thresh_fpd is None:
            logging.debug("Pathscore threshold is not defined")
            raise learning_exception

    def detect(self, datalist):
        u"""
            指定されたデータリストをFPDでスコア付けして、
            閾値で標準化を行う

            Parameters
            ----------
            datalist : list of string

            Returns
            -------
            scaled_score_list : list of float
                スコアリスト(各スコアは0～100点)
                正の値に大きいほど異常
        """

        # todo: 今後モデルなどを追加するときには、ココの構造を見やすく整理しないといけない
        if self.fbmodel is not None:
            # fpdモデルの基準パスにフィードバックモデルに登録された基準パスを追加する
            self.fpd.frequent_paths.extend(
                self.fpd.get_split_path_set(
                    self.fbmodel.frequent_paths_displayed
                    )
                )
        # raw_score_listにdatalistに対応した生スコアを格納
        raw_score_list = self.__detect(datalist)
        scaled_thresh_FPD = self.fpd.get_scaled_Threshold_fpd()
        if self.fbmodel is not None:
            raw_score_list = self.fbmodel.whitelist_feedback(
                datalist,
                raw_score_list,
                fixvalue=scaled_thresh_FPD
            )
        alpha = 1 / 100**(1 / (scaled_thresh_FPD - 2))
        scaled_score_list = [100 * alpha**(s - 2) for s in raw_score_list]

        return scaled_score_list

    def __detect(self, datalist):
        score_list = self.fpd.get_raw_path_scores(datalist)
        return score_list

    def save_fpd_file(self, path):
        u"""
            自身の全パラメタをファイルにセーブする。
            セーブしたファイルは、'load_fpd_file'によってロードできる。
        """

        savedata = SaveData(self)
        dill.dump(savedata, open(path, "wb"))

    def load_fpd_file(self, datasource_container, model_id, fsname):
        u"""
            'save_fpd_file'によってセーブしたファイルをロードする。
        """

        savedata = datasource_container.load_fpd_file(model_id, fsname)

        if (hasattr(savedata.fpd, "split_char_list")):
            self.fpd = savedata.fpd
        else:
            self.fpd = FPDModel(
                split_char_list=[re.escape("\\")],
                score_rate=savedata.fpd.score_rate,
                )
            self.fpd.frequent_paths = savedata.fpd.frequent_paths
            self.fpd.Thresh_fpd = savedata.fpd.Thresh_fpd


class SaveData(object):
    u"""
        'FpdDetector'のセーブとロードで扱うデータをまとめるためのクラス。
    """

    def __init__(self, fpd_detector: FpdDetector):
        self.fpd = fpd_detector.fpd


class FewDataException(Exception):
    pass


class LearningException(Exception):
    pass
