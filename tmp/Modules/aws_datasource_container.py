import os
import io
import glob
import tempfile
import shutil
import json
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from typing import Tuple, List, Dict
from Modules.detector.freq2 import FrequencyDetector, SaveData
from Modules.datasource_container import IDataSourceContainer
from model_db import AwsModelDb, ReportModelStatus
import Modules.file_system_name_db as fileSysNameDb
import score_db as scoredb
import dill
import boto3
import tempfile
import hashlib
import logging


class AwsDataSourceContainer(IDataSourceContainer):
    # Lambda関数のエフェメラルストレージ上限は10GBだが、若干余裕を持たせて8GBを上限に(byte換算)。
    MAX_STORAGE_SIZE = 8589934592

    def __init__(
        self,
        src_bucket_name: str,
        base_system_key: str,
        base_data_key: str,
        aws_id: str,
        mgmt_id: str,
        report_id,
        table_name: str,
        target_name: str,
        path_temp_score_db_key=None,
        freq_temp_score_db_key=None
    ):
        u"""
            AWS用のデータアクセス
        """

        self.src_bucket_name = src_bucket_name
        self.base_system_key = base_system_key
        self.base_data_key = base_data_key
        self.aws_id = aws_id
        self.mgmt_id = mgmt_id
        self.report_id = report_id
        self.table_name = table_name
        self.target_name = target_name
        self.model_target_name = ""
        self.old_model_target_name = ""
        self.path_temp_score_db_key = path_temp_score_db_key
        self.freq_temp_score_db_key = freq_temp_score_db_key

    def __enter__(self):
        self.aws_score_db = scoredb.AwsScoreDb(
            os.path.join(tempfile.gettempdir(), 'ALog'))
        # Lambdaのインスタンスを使いまわした際に、前回実行時のファイルが残存している可能性がある
        for file in os.listdir(tempfile.gettempdir()):
            path = os.path.join(tempfile.gettempdir(), file)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)
        return self

    def __exit__(self, ex_type, ex_value, trace):
        if not os.path.isdir(self.aws_score_db.output_dir):
            return
        s3 = boto3.client('s3')
        for kind in os.listdir(self.aws_score_db.output_dir):
            if kind == 'path':
                for file in os.listdir(os.path.join(self.aws_score_db.output_dir, kind)):
                    date = file.rstrip(".csv")
                    s3.upload_file(os.path.join(self.aws_score_db.output_dir, kind, file), self.src_bucket_name,
                                   self.__get_temp_path_score_path(date, self.get_file_sys_name()))
            if kind == 'freq':
                for file in os.listdir(os.path.join(self.aws_score_db.output_dir, kind)):
                    date = file.rstrip(".csv")
                    s3.upload_file(os.path.join(self.aws_score_db.output_dir, kind, file), self.src_bucket_name,
                                   self.__get_temp_freq_score_path(date, self.get_file_sys_name()))

    def save_model_data(self, status):
        # モデル情報の保存は別の場所で行う
        # AwsModelDb(
        #     self.aws_id,
        #     self.mgmt_id,
        #     self.table_name
        # ).update_report_model(status)
        # logging.info("Updated ModelDb.")
        return

    def save_fpd_file(self, fpd_detector, model_id, fsname):
        # ALog Cloudではtarget_nameで保存する
        detectorpath = self.get_target_model_path(model_id, self.get_file_sys_name())

        dill_buffer = io.BytesIO()
        dill.dump(fpd_detector, dill_buffer)
        s3 = boto3.resource('s3')
        s3.Bucket(self.src_bucket_name).put_object(
            Key=detectorpath, Body=dill_buffer.getvalue())

    def load_fpd_file(self, model_id, fsname):
        path = self.get_target_model_path(model_id, self.get_file_sys_name())
        with io.BytesIO() as data:
            s3 = boto3.resource('s3')
            try:
                s3.Bucket(self.src_bucket_name).download_fileobj(path, data)
                data.seek(0)
                return dill.load(data)
            except Exception as e:
                pass
        # 59048の修正後に学習を行っていない場合、古いモデルを読み込む必要がある
        old_path = self.get_target_model_path(model_id, self.__get_old_model_target_name(model_id))
        with io.BytesIO() as old_data:
            try:
                s3.Bucket(self.src_bucket_name).download_fileobj(old_path, old_data)
                old_data.seek(0)
                return dill.load(old_data)
            except Exception as e:
                return None

    def save_topic_model_file(self, model_id, lda, filename):
        # ここでは実装しない
        return

    def save_target_layout_file(self, word_layout, model_id, fsname):
        # ここでは実装しない
        return

    def save_freq_model_file(self, model_id, fsname, hour, detector):
        model_path = self.__get_usermodel_filepath(
            model_id,
            self.get_file_sys_name(),
            hour
        )
        savedata = SaveData(detector)

        dill_buffer = io.BytesIO()
        dill.dump(savedata, dill_buffer)
        s3 = boto3.resource('s3')
        s3.Bucket(self.src_bucket_name).put_object(
            Key=model_path, Body=dill_buffer.getvalue())

    def load_freq_model_file(
        self,
        model_id,
        fsname: str,
        min_prob_dens
    ) -> Dict[int, FrequencyDetector]:
        self.model_target_name = self.__get_model_target_name(model_id)
        s3 = boto3.resource('s3')

        detectors = {}
        for hour in range(0, 24):
            try:
                model_path = self.__get_usermodel_filepath(
                    model_id, self.model_target_name, hour)
                with io.BytesIO() as data:
                    s3.Bucket(self.src_bucket_name).download_fileobj(
                        model_path, data)
                    data.seek(0)
                    savedata = dill.load(data)
            except Exception as e:
                logging.warning(f"DEBUG: load_freq_model_file: Failed to load model: {str(e)}")
                # 59048の修正後に学習を行っていない場合、古いモデルを読み込む必要がある
                try:
                    model_path = self.__get_usermodel_filepath(
                        model_id, self.__get_old_model_target_name(model_id), hour)
                    with io.BytesIO() as data:
                        s3.Bucket(self.src_bucket_name).download_fileobj(
                            model_path, data)
                        data.seek(0)
                        savedata = dill.load(data)
                except Exception as e:
                    logging.warning(f"DEBUG: load_freq_model_file: Failed to load OLD model: {str(e)}")
                    logging.info(f"DEBUG: load_freq_model_file: No model found for hour {hour}, model_id: {model_id}, target_name: {self.model_target_name}.")
                    return
            if not model_path:
                detectors[hour] = None
                continue
            detector = FrequencyDetector(
                min_prob_dens=min_prob_dens
            )
            savedata.set_params(detector)
            detectors[hour] = detector
        return detectors

    def load_fbmodel_file(self, report_id, fsname):
        path = self.__get_fbmodel_filepath(report_id, self.get_file_sys_name())
        s3 = boto3.resource('s3')
        with io.BytesIO() as data:
            try:
                s3.Bucket(self.src_bucket_name).download_fileobj(path, data)
                data.seek(0)
                return dill.load(data)
            except:
                pass
        # 59048の修正後に学習を行っていない場合、古いモデルを読み込む必要がある
        old_path = self.__get_fbmodel_filepath(report_id, self.old_model_target_name)
        with io.BytesIO() as old_data:
            try:
                s3.Bucket(self.src_bucket_name).download_fileobj(old_path, old_data)
                old_data.seek(0)
                return dill.load(old_data)
            except:
                return None

    def save_model_contour(self, model_id, target_name, date, contour_data):
        data_path = self.__get_freq_contour_filepath(model_id, self.get_file_sys_name(), date)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.src_bucket_name)
        obj = bucket.Object(data_path)
        obj.put(Body=json.dumps(contour_data))

    def save_path_score(self, path_score_summary):
        self.aws_score_db.store_path_score(path_score_summary)

    def save_freq_score(self, freq_score_summary):
        self.aws_score_db.store_freqscore(freq_score_summary)

    # SaaS版ではローカルにある結果を使用する
    def get_freq_score(self, fsname, date):
        filename = datetime.strftime(date, "%Y%m%d") + ".csv"
        filepath = os.path.join(self.aws_score_db.output_dir, "freq", filename)
        return self.aws_score_db.get_freqscore_from_local(filepath, self.target_name, date, self.report_id)

    def delete_path_score(self, reportid):
        return
        self.aws_score_db.delete_all_score(reportid)

    def delete_freq_score(self, reportid):
        return
        self.aws_score_db.delete_all_score(reportid)

    def delete_old_pathscore(self, detected_date, current_time, report_id):
        return
        self.aws_score_db.delete_old_score(
            detected_date, current_time, report_id)

    def delete_old_freqscore(self, detected_date, current_time, report_id):
        return
        self.aws_score_db.delete_old_score(
            detected_date, current_time, report_id)

    def collect_target_fsname_and_csv_file_paths(
        self,
        report_id
    ) -> List[Tuple[str, List[str]]]:
        u"""
            作業ディレクトリのファイル群を、スコア算出対象毎に仕分けて返す

            Returns
            -------
                list of tuple(file_sys_name: str, csv_file_paths: list of str)
        """
        file_sys_names = self.__collect_target_names(report_id)

        return [
            (
                file_sys_name,
                [os.path.join(tempfile.gettempdir(), path)
                 for path in self.__collect_target_csv_file_paths(report_id, file_sys_name)]
            )
            for file_sys_name in file_sys_names
        ]

    def __collect_target_names(self, report_id) -> List[str]:
        u"""
            Returns
            -------
                スコア算出対象のファイルシステム上の名前リスト : list of str

            Notes
            -----
                訓練データのディレクトリは
                {report_id}/{file_sys_name}/{date}.csv
                となっているため、{report_id}フォルダ直下の
                フォルダ名を返している
        """
        # 今はreport_idはないので、ひとまずこれで
        return os.listdir(self.get_target_csv_file_dir_path(report_id))

    def get_target_csv_file_dir_path(self, report_id) -> str:
        return os.path.join(tempfile.gettempdir(), self.base_data_key)

    def get_target_model_dir_path(self, model_id: str, fsname: str) -> str:
        return "/".join([self.base_system_key.rstrip('/'), "reportModels", model_id, fsname])

    def __collect_target_csv_file_paths(
        self,
        report_id,
        file_sys_name: str
    ) -> List[str]:
        u"""
            Parameters
            ----------
                file_sys_name: str
                    スコア算出対象のファイルシステム上の名前

            Returns
            -------
                スコア算出対象に関するCSVファイルパスのリスト: list of str
        """
        # 学習データの出力の仕方によって以下は変更する必要あり
        return glob.glob(
            os.path.join(
                # note: globでは角括弧[]をエスケープしないといけない
                glob.escape(self.get_target_csv_file_dir_path(report_id)),
                glob.escape(file_sys_name),
                '*.csv'
            )
        )

    def get_target_model_path(self, model_id: str, fsname: str) -> str:
        return "/".join([self.get_target_model_dir_path(model_id, fsname), "pathdetector"])

    def get_models_dir_path(self, model_id) -> str:
        return "/".join([self.base_system_key.rstrip('/'), "reportModels", model_id])

    def __get_topic_model_filepath(self, model_id, filename) -> str:
        return "/".join([self.get_models_dir_path(model_id), "topicmodel", filename])

    def __get_fbmodel_filepath(self, report_id, fsname):
        return "/".join([self.base_system_key.rstrip('/'), "feedbackModel", report_id, fsname, "feedbackmodel"])

    def __get_freq_score_path(self, date):
        str_date = datetime.strftime(date, "%Y%m%d")
        return "/".join([self.base_system_key.rstrip('/'), "score", "frequency", f"{str_date}.csv"])

    def __get_freq_contour_filepath(self, model_id, fsname, date):
        str_date = datetime.strftime(date, "%Y%m%d")
        return "/".join([self.get_target_model_dir_path(model_id, fsname), "freqContour", f"{str_date}.json"])

    def __get_temp_path_score_path(self, str_date, fsname):
        return "/".join([self.path_temp_score_db_key, str_date, f"{fsname}.csv"])

    def __get_temp_freq_score_path(self, str_date, fsname):
        return "/".join([self.freq_temp_score_db_key, str_date, f"{fsname}.csv"])

    def get_reportmodel(
        self,
        report_id: int,
        risk_kind: int
    ) -> ReportModelStatus:
        return AwsModelDb(
            self.aws_id, self.mgmt_id, self.table_name
        ).get_cur_report_model(report_id, risk_kind)

    def __get_usermodel_filepath(self, model_id, fsname, hour) -> str:
        return "/".join([self.get_target_model_dir_path(model_id, fsname), str(hour)])

    # target_nameとして渡されているのでそれを返す
    # Cloudではハッシュ名で扱う
    def get_file_sys_name(self, target_name=None):
        if target_name == None:
            return hashlib.sha1(self.target_name.lower().encode()).hexdigest()
        else:
            return hashlib.sha1(target_name.lower().encode()).hexdigest()

    def get_target_name(self, file_sys_name=None):
        return self.target_name

    # 実際に保存されているモデルのfile_sys_nameを取得する
    def __get_model_target_name(self, model_id):
        return self.get_file_sys_name()

    # 59048修正前に使われていたモデル保存名
    # 現在は必要ないが、アップデート前のモデルを読み込む際に使用する
    def __get_old_model_target_name(self, model_id):
        prefix = "/".join([self.base_system_key.rstrip('/'), "reportModels", model_id]) + "/"
        keys = self.__get_all_folders(prefix)
        # 59048修正前はtarget_nameは改行文字をエスケープして200文字以下にカットされていたのでカットする
        escaped_target_name = self.target_name.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\r")
        if len(escaped_target_name) >= 200:
            lower_target_name = escaped_target_name[:200].lower()
        else:
            lower_target_name = escaped_target_name.lower()
        for key in keys:
            # /{model_id}/{target_name}/
            s3_target_name = key.split("/")[-2]
            lower_s3_target_name = s3_target_name.lower()
            if lower_s3_target_name == lower_target_name:
                self.old_model_target_name = s3_target_name
                return s3_target_name
        return ""

    def __get_all_folders(self, prefix: str, keys: List = None, marker: str = ''):
        s3 = boto3.client('s3')
        response = s3.list_objects(
            Bucket=self.src_bucket_name, Prefix=prefix, Marker=marker, Delimiter='/')

        if keys is None:
            keys = []

        if 'CommonPrefixes' in response:
            keys.extend([content['Prefix']
                        for content in response['CommonPrefixes']])
        if 'Contents' in response:
            last_file_key = response['Contents'][-1]['Key']
            if 'IsTruncated' in response:
                return self.__get_all_folders(prefix=prefix, keys=keys, marker=last_file_key)
        return keys

    def check_fpd_file(self, model_id, fsname):
        self.model_target_name = self.__get_model_target_name(model_id)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.src_bucket_name)
        prefix = self.get_target_model_dir_path(model_id, self.model_target_name) + "/"
        objs = bucket.objects.filter(Prefix=prefix)
        if list(objs.limit(1)):
            return True
        else:
            # 59048の修正でモデルのキー名がハッシュ化されたものに変更されるが、それがなかった場合に変更前のものを読み取れるようにする必要がある
            prefix = self.get_target_model_dir_path(model_id, self.__get_old_model_target_name(model_id)) + "/"
            old_objs = bucket.objects.filter(Prefix=prefix)
            if list(old_objs.limit(1)):
                return True
            return False

    def set_dataset(self, logger):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.src_bucket_name)

        local_dir = os.path.join(tempfile.gettempdir(), self.base_data_key, self.get_file_sys_name())
        os.makedirs(local_dir, exist_ok=True)
        size = 0
        objs_descending_by_date = sorted(bucket.objects.filter(Prefix=self.base_data_key+f"/{self.get_file_sys_name()}/"), key=lambda o: o.key.split("/")[-1], reverse=True)

        for obj in objs_descending_by_date:
            key = obj.key
            if key[-1] == "/":
                # フォルダなのでskip
                continue

            if size + obj.size > self.MAX_STORAGE_SIZE:
                logger.info(f"skip data due to exceeding the capacity of ephemeral storage. [target_name: {self.target_name}][obj_key: {key}][obj_size: {obj.size} byte][stored_size: {size} byte]")
            else:
                filename = key.split("/")[-1]
                logger.info(f"start downloading data. [target_name: {self.target_name}][obj_key: {key}][obj_size: {obj.size} byte]")

                local_filepath = os.path.join(local_dir, filename)
                bucket.download_file(key, local_filepath)

                size += obj.size
                logger.info(f"end downloadeing data. [target_name: {self.target_name}][obj_key: {key}][obj_size: {obj.size} byte][stored_size: {size} byte]")
