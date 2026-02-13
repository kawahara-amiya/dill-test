from Modules.detector.base import BaseDetector
import math
from scipy import stats
from scipy.stats import gaussian_kde
import dill
import sys
import numpy as np
import logging


class FrequencyDetector(BaseDetector):
    u"""docstring for FrequencyDetector
        scipyのgaussian_kdeを利用した頻度異常検知器。
        確率密度関数から負対数尤度を出して正規化してスコアリングする。
        データは指定した正規分布により膨らませて学習させることにより、元分布の性質を仮定させられる。
    """

    def __init__(
        self,
        bw_method="scott",
        inflate_scale: float = 10,
        inflate_size: int = 0,
        inflate_model=stats.norm,
        min_prob_dens: float = sys.float_info.min,
        normalize_score: bool = True
    ):
        u"""
            コンストラクト後、まず 'learn' を行うか 'load_file' を行うことを想定。
            Parameters
            ----------
                bw_method : str or function
                    scipy の gaussian_kde の bw_method
                inflate_scale : 0より大きい float, default 10
                    値膨らましのスケール。正規分布の場合は標準偏差。
                    値がどの程度ブレ得るものかという観点で、検知の鋭さを調整できる。
                inflate_size : int, default 0
                    値膨らましのサイズ。1個の入力値をこのサイズに膨らませる。
                    0を指定すると、値膨らましを行わない。
                inflate_model : scipy.stats の各種統計モデルクラス, defalut norm
                    値膨らましのモデル。デフォルト値で正規分布。
                min_prob_dens : float, default 1e-15
                    負対数尤度を計算する際、これを下回る確率密度はこの最小値に丸められて計算される。
                    この最小値が、異常スコアの最大値を決定し、それが正規化の際の異常スコア100%になる。
                    どのくらいのレアケースを異常とするかという観点で、検知の鋭さを調整できる。
                normalize_score : bool, default True
                    スコアの正規化を行うかどうか。
                    行わない場合、確率密度の負対数尤度をそのままスコアとして返す。
        """

        super(FrequencyDetector, self).__init__()
        self.bw_method = bw_method  # gaussian_kde に用いる bw_method
        self.inflate_size = inflate_size
        self.inflate_scale = inflate_scale
        self.inflate_model = inflate_model
        self.min_prob_dens = min_prob_dens
        self.prob_dens_func = None  # 確率密度関数
        self.min_handle = 0  # この検知器が扱うべき最小値
        self.max_handle = 0  # この検知器が扱うべき最大値
        self.normalize_score = normalize_score

    def learn(self, datalist):
        u"""
            指定したデータリストから確率密度分布を推定する。
            コンストラクト時に指定したパラメタにより、データリストの膨らましを行う。

            Notes
            -----
                用いているライブラリの仕様上、入力値がすべて同じ値であると推定が上手くいかないので、
                その場合はややズラした値を自動で加えて補正する場合がある。
        """

        if len(datalist) == 0:
            raise Exception("Error: Datalist length is zero.")
        try:
            logging.info(f"DEBUG: learn: datalist before inflate: {datalist}")
            inflated_list = self.__inflate(datalist)
            logging.info(f"DEBUG: learn: datalist after inflate: {inflated_list}")
            # なにもしていない可能性がある、整数値倍
            self.prob_dens_func = gaussian_kde(
                inflated_list, bw_method=self.bw_method)
            self.__set_handle_minmax(inflated_list)
        except Exception:
            # 全値が等しい場合、gaussian_kdeが行列計算でエラるので、ややズラした値を加えて再試行
            # inflate処理を加えたので、ほぼ起こり得ないはずだが、sizeを1や0に設定した場合には発生し得るかも
            logging.warning("DEBUG: error occurred in inflation")
            fixed_datalist = inflated_list + \
                [
                    inflated_list[0] * 1.001,
                    inflated_list[0] * 0.999,
                    inflated_list[0] + 1e-1
                ]
            logging.info(f"DEBUG: learn: fixed_datalist: {fixed_datalist}")
            self.prob_dens_func = gaussian_kde(
                fixed_datalist, bw_method=self.bw_method)
            self.__set_handle_minmax(fixed_datalist)

    def detect(self, datalist) -> list:
        u"""
            指定したデータリストの各データの異常スコアを算出して返す。
            異常スコアは0から1の間であり、大きいほど異常であることを示す。
            スコア自体は、推定した確率密度の負の対数尤度の値を正規化したもの。
            事前に 'learn' か 'load_file' を行うこと。

            Returns
            -------
                異常スコアの list
        """

        return [
            self.__score(v if not isinstance(v, np.ndarray) else v[0])
            for data in datalist
            for v in [self.prob_dens_func(data)]
        ]

    # def save_file(self, path):
    #     u"""
    #         自身の全パラメタをファイルにセーブする。
    #         セーブしたファイルは、'load_file'によってロードできる。
    #     """

    #     savedata = SaveData(self)
    #     dill.dump(savedata, open(path, "wb"))

    # def load_file(self, path):
    #     u"""
    #         'save_file'によってセーブしたファイルをロードする。
    #     """

    #     savedata = dill.load(open(path, "rb"))
    #     savedata.set_params(self)

    def __set_handle_minmax(self, datalist):
        u"""
            指定されたデータリストから、扱うべき最小値と最大値を算出して自身のメンバにセットする。
        """

        self.min_handle = 0.0

        max_value = max(datalist)
        logging.info(f"DEBUG: __set_handle_minmax: initial max_value: {max_value}")

        v = max_value
        c = 0
        while True:
            c += 1
            if c > 30:
                break

            v = v * 1.5
            s = self.detect([v])[0]
            logging.info(f"DEBUG: __set_handle_minmax: detect score for value {v}: {s}")
            if s >= 1.0:
                break
        self.max_handle = v

    def __inflate(self, datalist: list) -> list:
        u"""
            値の膨らましを行う。これにより、検知の鋭さを調整することができる。
        """

        if self.inflate_size == 0:
            return datalist

        result = []
        for data in datalist:
            samples = list(self.inflate_model.rvs(
                loc=data, scale=self.inflate_scale, size=self.inflate_size))
            result += [s for s in samples if s >= 0]
        return result

    def __score(self, prob_dens: float) -> float:
        u"""
            確率密度に対して負の対数尤度を取り、それを0から1の範囲に正規化したものをスコアとする。
        """

        def negloglike(value):
            return -1 * math.log(value)

        if not self.normalize_score:
            logging.info("DEBUG: __score: self.normalize_score is False, returning negloglike directly")
            return negloglike(prob_dens)

        if prob_dens <= self.min_prob_dens:
            return 1.0

        # prob_dens can be larger than 1.
        if prob_dens >= 1.0:
            return 0

        logging.info(f"DEBUG: __score:, min_prob_dens: {self.min_prob_dens}")
        max_negloglike = negloglike(self.min_prob_dens)

        # ここnegloglikeに渡しているデータがfloatではなくリストの可能性がある
        logging.info(f"DEBUG: __score: prob_dens type: {type(prob_dens)} value: {prob_dens}, max_negloglike: {max_negloglike}")
        return float(negloglike(prob_dens) / max_negloglike)

    def filter(self, frequency_datalist: list, sigma_scale: int) -> list:
        u"""filter data
            異常値を学習モデルに含めないために標準偏差でフィルターする

            Parameters
            ----------
            sigma_scale : フィルターを行うための標準偏差の倍率。ReportConfigでのデフォルトは3。
        """
        ave, sigma = stats.norm.fit(frequency_datalist)

        if sigma == 0:
            return frequency_datalist

        filter_range = sigma * sigma_scale
        threshold_plus = int(np.ceil(ave + filter_range))
        threshold_minus = int(np.ceil(ave - filter_range))
        normalization_datalist = [
            frequency_data for frequency_data in frequency_datalist
            if threshold_minus <= frequency_data
            and frequency_data <= threshold_plus
        ]

        if not any(normalization_datalist):
            logging.debug((
                f"filtering result is none."
                f"threshold_minus:{threshold_minus},"
                f"threshold_plus:{threshold_plus}"
            ))
            return frequency_datalist

        return normalization_datalist


class SaveData(object):
    u"""docstring for SaveData
        'FrequencyDetector'のセーブとロードで扱うデータをまとめるためのクラス。
    """

    def __init__(self, freq_detector: FrequencyDetector):
        self.prob_dens_func = freq_detector.prob_dens_func
        self.bw_method = freq_detector.bw_method
        self.min_handle = freq_detector.min_handle
        self.max_handle = freq_detector.max_handle
        self.inflate_size = freq_detector.inflate_size
        self.inflate_scale = freq_detector.inflate_scale
        self.inflate_model = freq_detector.inflate_model
        self.min_prob_dens = freq_detector.min_prob_dens
        self.normalize_score = freq_detector.normalize_score

    def set_params(self, freq_detector: FrequencyDetector):
        freq_detector.prob_dens_func = self.prob_dens_func
        freq_detector.bw_method = self.bw_method
        freq_detector.min_handle = self.min_handle
        freq_detector.max_handle = self.max_handle
        freq_detector.inflate_size = self.inflate_size
        freq_detector.inflate_scale = self.inflate_scale
        freq_detector.inflate_model = self.inflate_model
        freq_detector.min_prob_dens = self.min_prob_dens
        freq_detector.normalize_score = self.normalize_score
