from typing import List
import random
import re
import math
from statistics import mean


class FPDModel:
    u"""ユーザーのアクセスログをもとにFPD(File Path Diversity)を算出するためのモデル
    """

    def __init__(
        self,
        split_char_list,
        score_rate: int = 4
    ):
        self.split_char_list = split_char_list
        self.cutoff = None
        self.score_rate = score_rate
        self.frequent_paths: List[List[str]] = None
        self.Thresh_fpd = None

    def set_frequent_paths(
        self,
        array_of_paths,
        sample_num=None,
        sampling_iteration=20,
        true_path_floor=8
    ):
        u"""
            アクセスログ群から中心となるパス(基準パス)を抽出する
            ログ数が多い場合、sample_numを指定することでサンプリングして抽出することで
            高速化を行うことができる

            Parameters
            ----------
                array_of_paths : list of string
                    アクセスログのリスト
                sample_num : int, option
                    サンプリングを行う場合のサンプリング数
                sampling_iteration : int, option
                    サンプリングにより基準パス候補の抽出を行う回数
                true_path_floor : int、option
                    サンプリングを行う場合の抽出下限値
                    上記のサンプリング回数のうち、何回以上候補に残ったものを基準パスにするかを決定する
        """

        # サンプリングを用いて基準パスを抽出する場合、はじめにsampling_iterationの数値の回数だけ基準パス候補の抽出を行う
        # その中で、true_path_floorの数値以上の回数基準パスの候補になったパスを基準パスとする
        # デフォルトでは20回ほどサンプリングしたアクセスログで基準パス候補の抽出を行い、その中で8回以上抽出されたものを基準パスとする
        # これはサンプリングによる抽出の誤差を低減するためである
        # アクセスログの母数とサンプリング数にもよるが、サンプリングを行わない場合に抽出される基準パスの内約95%は抽出されるようになる
        if sample_num:
            true_path_count = {}
            true_paths = []
            for _ in range(sampling_iteration):
                sampling_filepath = \
                    random.sample(list(array_of_paths), sample_num)
                temporary_true_paths = self.get_frequent_paths(
                    sampling_filepath)
                if temporary_true_paths is None:
                    continue
                for split_path in temporary_true_paths:
                    filepath = '\\'.join(split_path)
                    if filepath in true_path_count:
                        true_path_count[filepath] += 1
                    else:
                        true_path_count[filepath] = 1
            for path, count in true_path_count.items():
                if count >= true_path_floor:
                    true_paths.append(list(path.split('\\')))
            if len(true_paths) != 0:
                self.frequent_paths = true_paths
        else:
            self.frequent_paths = self.get_frequent_paths(list(array_of_paths))

    def get_frequent_paths(self, array_of_paths) -> List[List[str]]:
        u"""
            ディレクトリで分割した基準パスのリストを取得する
            詳細な説明は#30491を参照

            Parameters
            ----------
                array_of_paths : List of string
                    アクセスログのリスト
            Returns
            -------
                list of (list of string)
        """

        split_filepaths = self.get_split_path_set(array_of_paths)
        num_path = len(array_of_paths)

        # cutoffの値以上の出現回数をもつディレクトリを基準パスの候補として抽出する
        candidate_paths = set()

        # ディレクトリ構造によるソートを行い、パスの抽出を効率的に行う
        sorted_split_filepaths = sorted(split_filepaths)
        for i, split_path_i in enumerate(sorted_split_filepaths):
            # 注目しているパスについて、ソートされたリストにおいてcutoff-1分だけ下のパス(comparing_num番目のパス)との比較を行うことで基準パスの候補を抽出する
            # 比較すべきパスが下に存在しない場合、そのパスから基準パスの候補は抽出されないのでそこで処理を終える
            comparing_num = i + self.cutoff - 1
            if comparing_num == num_path:
                break

            # 注目しているi番目のパスのj番目のディレクトリについて、比較するパスのj番目のディレクトリと一致するかどうかを調べる
            # これを満たす最も深いディレクトリを基準パスの候補に加える
            satisfied_directory_len = 0
            for j, directory_i_j in enumerate(split_path_i):
                if j < len(sorted_split_filepaths[comparing_num]) \
                    and directory_i_j == \
                        sorted_split_filepaths[comparing_num][j]:
                    satisfied_directory_len += 1
                else:
                    break
            candidate_path = '\\'.join(
                sorted_split_filepaths[i][:satisfied_directory_len])
            if satisfied_directory_len == 0 \
                    or candidate_path in candidate_paths:
                continue
            else:
                candidate_paths.add(candidate_path)

        if len(candidate_paths) == 0:
            return

        true_paths = []

        # サブディレクトリをもつパスを候補から除外する
        # ソートを行うことにより、次のことが言える
        # ・注目しているパスの上にはそのパスのサブディレクトリは存在しない
        # ・注目しているパスの1個下のパスがサブディレクトリでなければ、それ以降のパスはそのパスのサブディレクトリではない
        # ・注目しているパスの1個下のパスがそのパスより短ければ、そのパスは注目しているパスのサブディレクトリではない
        # したがってリストにおいて注目しているパスとその1個下のパスを比較することで、そのパスがサブディレクトリをもつかどうかを判断することができる
        candidate_paths = list(candidate_paths)
        candidate_paths.sort()
        for i in range(len(candidate_paths)-1):
            if len(candidate_paths[i]) > len(candidate_paths[i + 1]) or \
                    candidate_paths[i] != \
                    candidate_paths[i + 1][:len(candidate_paths[i])]:
                true_paths.append(candidate_paths[i].split('\\'))
        # ソートを行った場合、最後のパスは必ず基準パスとなる
        true_paths.append(candidate_paths[-1].split('\\'))
        return true_paths

    def get_path_scores(self, raw_scores) -> List[float]:

        return [self.get_path_score(raw_score) for raw_score in raw_scores]

    def get_path_score(self, raw_score) -> float:
        u"""
            パスの生スコアからスコアを計算する

            Parameters
            ----------
                raw_score: int
                    パスの生スコア

            Returns
            -------
                float
                    パスのFPD計算用スコア
        """
        if raw_score <= -20:
            score = 0
        else:
            score = self.score_rate**(2 * raw_score)
        return score

    def get_raw_path_scores(self, array_of_paths) -> List[int]:
        u"""
                与えられたパス群について、生スコアのリストを返す

                Parameters
                ----------
                    array_of_paths: list of string
                        ファイルパスのリスト

                Returns
                -------
                    list of int
        """

        return [self.get_raw_path_score(filepath)
                for filepath in array_of_paths]

    def get_raw_path_score(self, filepath) -> int:
        u"""
            与えられたパスについて、生スコアを返す
            このスコアは元論文における指数項3-kと一致する
            ただし、scoreが0の場合は例外的に-20を返すものとする

            Parameters
            ----------
                filepath: string
                    ファイルパス

            Returns
            -------
                float
        """
        split_check_path = self.get_split_path(filepath)

        temp_raw_score = []
        for split_freq_path in self.frequent_paths:
            k = 1
            for i in range(min(len(split_freq_path), len(split_check_path))):
                if (split_freq_path[i] == split_check_path[i]):
                    k += 1
                else:
                    break
            if (k-1 == len(split_check_path) or k-1 == len(split_freq_path)):
                diff = -20  # 定義上は-∞に相当するのだが、扱いにくいので-20とする
            else:
                diff = 3 - k
            temp_raw_score.append(diff)
        return min(temp_raw_score)

    def set_Threshold_fpd(
        self,
        array_of_paths,
        sampling_num=100,
        sampling_set_num=10,
        seed=0
    ):
        u"""
            アクセスログからセットをいくつか作成し、そのセットを用いて異常検知の閾値を決定する
            ここでは各セットのFPDの平均を閾値とする

            Parameters
            ----------
                array_of_paths: list of string
                    アクセスログのリスト
                sampling_num: int
                    サンプリング時の1セットのパスの数
                sampling_set_num: int
                    サンプリング時のセットの数
                seed: int
                    サンプリング時の初期シードの値
        """
        random.seed(seed)
        sample_path_set = []
        for _ in range(sampling_set_num):
            sample_path_set.append(
                random.sample(list(array_of_paths), sampling_num))
        all_normal_fpd = []
        for sample_paths in sample_path_set:
            all_normal_fpd.append(
                FPDModel.get_fpd(
                    self.get_path_scores(
                        self.get_raw_path_scores(sample_paths)
                    )
                )
            )
        thresh_fpd = mean(all_normal_fpd)
        if math.isclose(
                self.get_raw_score_from_score(thresh_fpd), 2, rel_tol=0.01):
            return
        self.Thresh_fpd = thresh_fpd

    def get_scaled_Threshold_fpd(self):
        return self.get_raw_score_from_score(self.Thresh_fpd)

    def get_raw_score_from_score(self, score) -> int:
        u"""
            パスのスコアを元の生スコアに変換する
            ただし、scoreが0の場合は例外的に-20を返すものとする

            Parameters
            ----------
                score: float
                    パスのスコア

            Returns
            -------
                float
                    パスの生スコア
        """
        if score == 0:
            raw_score = -20
        else:
            raw_score = math.log(score, self.score_rate) / 2
        return raw_score

    def get_split_path_set(self, path_set) -> List[List[str]]:
        u"""
            フルパスのリストからディレクトリごとに分割処理をしたパスのリストのセットを返す

            Parameters
            ----------
                path_set: list of string
                    フルパスのリスト

            Returns
            -------
                list of SplitFilepath
                    ディレクトリごとに分割処理をしたパスのリスト
        """

        return [self.get_split_path(path) for path in path_set]

    def get_split_path(self, path) -> List[str]:
        u"""
            フルパスからディレクトリごとに分割処理をしたパスのリストを返す

            Parameters
            ----------
                path: string
                    フルパス

            Returns
            -------
                list of string
                ディレクトリごとに分割処理をしたパス
        """

        split_rep = "|".join(self.split_char_list)

        split_path = [directory for directory in re.split(split_rep, path)
                      if directory.strip() != ""]
        if split_path[0] in ("http:", "https:"):
            del split_path[0]
        return split_path

    @staticmethod
    def get_fpd(scores) -> float:
        u"""
            与えられたスコアのリストからFPDを算出して返す
                FPD = 1 / n * Σ(p_i - p_freq)^2
            ただし、ここではスコア自体がすでに2乗されたものなので、平均したものを返すだけである

            Parameters
            ----------
                scores : list of float
                    対象となるパスのスコアのリスト

            Returns
            -------
                float
                    FPDスコア
        """
        FPD = sum(scores) / len(scores)
        return FPD
