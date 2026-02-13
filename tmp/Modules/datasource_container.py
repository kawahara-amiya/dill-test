import uuid
import logging
from abc import ABCMeta, abstractmethod

class IDataSourceContainer(metaclass=ABCMeta):
    @abstractmethod
    def save_model_data(self, status):
        pass

    @abstractmethod
    def save_fpd_file(self, fpd_detector, model_id, fsname):
        pass

    @abstractmethod
    def load_fpd_file(self, path):
        pass

    @abstractmethod
    def save_topic_model_file(self,  model_id, lda, filename):
        pass

    @abstractmethod
    def save_target_layout_file(self, word_layout, model_id, fsname):
        pass

    @abstractmethod
    def save_freq_model_file(self, fsname, hour, detector):
        pass

    @abstractmethod
    def load_freq_model_file(self, fsname, min_prob_dens):
        pass

    @abstractmethod
    def load_fbmodel_file(self, report_id, fsname):
        pass

    @abstractmethod
    def save_model_contour(self, fsname):
        pass

    @abstractmethod
    def save_path_score(self, score_summary):
        pass

    @abstractmethod
    def save_freq_score(self, score_summary):
        pass

    @abstractmethod
    def get_freq_score(self, fsname):
        pass

    @abstractmethod
    def delete_path_score(self, reportid: int):
        pass

    @abstractmethod
    def delete_freq_score(self, reportid: int):
        pass

    @abstractmethod
    def delete_old_pathscore(self, detected_date, current_time, report_id):
        pass

    @abstractmethod
    def delete_old_freqscore(self, detected_date, current_time, report_id):
        pass

    @abstractmethod
    def get_target_model_dir_path(self, model_id: str, fsname: str):
        pass

    @abstractmethod
    def get_target_model_path(self, model_id: str, fsname: str):
        pass

    @abstractmethod
    def get_models_dir_path(self, model_id):
        pass

    # @abstractmethod
    # def get_topic_model_filepath(self, model_id):
    #     pass

    # @abstractmethod
    # def get_fbmodel_filepath(self, report_id, fsname):
    #     pass

    @abstractmethod
    def get_reportmodel(self, report_id: int, risk_kind: int):
        pass

    # @abstractmethod
    # def get_usermodel_filepath(self, fsname, hour):
    #     pass

    @abstractmethod
    def get_file_sys_name(self, target_name=None):
        pass

    @abstractmethod
    def get_target_name(self, file_sys_name=None):
        pass

    @abstractmethod
    def check_fpd_file(self, model_id: str, file_sys_name: str):
        pass
