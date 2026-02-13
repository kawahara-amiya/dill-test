import io
import boto3
import dill
import logging


class FeedbackModel(object):

    # whitelistは各reportの各userごとに存在する

    def __init__(
        self,
        fbmodel_data_path,
        report_id,
        fsname,
        whitelist=[],
        frequent_paths=[]
    ):
        self.fbmodel_data_path = fbmodel_data_path
        self.report_id = str(report_id)
        self.fsname = fsname
        self.whitelist = list(set(whitelist))
        # frequent_paths_displayedにはユーザが入力したパス名が分割しないまま保存される。
        # 分割したパスが保存されるfrequent_pathsだけだと、もともとパスに'/'が先頭についたかどうかの情報がなくなるため、このようなフィールドを設けている
        self.frequent_paths_displayed = list(set(frequent_paths))

    def save_fb_file(self, src_backet_name):
        savedata = self
        path = "/".join([
            self.fbmodel_data_path,
            self.report_id,
            self.fsname,
            "feedbackmodel"
        ])

        dill_buffer = io.BytesIO()
        dill.dump(savedata, dill_buffer)
        s3 = boto3.resource('s3')
        s3.Bucket(src_backet_name).put_object(
            Key=path, Body=dill_buffer.getvalue())

        logging.info(
            "Feedbackmodel save successed. "
            f"WHITEPATH_COUNT:{len(savedata.whitelist)} "
            f"FREQUENTPATH_COUNT:{len(savedata.frequent_paths_displayed)}")
        logging.debug(
            f"WHITEPATH_LIST...{savedata.whitelist}")
        logging.debug(
            f"FREQUENTPATH_LIST...{savedata.frequent_paths_displayed}")

    def whitelist_feedback(
        self,
        datalist,
        score_list,
        fixvalue=-20
    ):
        for i in range(len(datalist)):
            if datalist[i] in self.whitelist:
                raw_score_before = score_list[i]
                score_list[i] = fixvalue
                logging.debug(f"{datalist[i]} is in whitelist!!!")
                logging.debug(
                    "raw score fixed...   " +
                    f"{raw_score_before} >> {fixvalue}"
                )
        return score_list

    def add_whitepath_and_save_fb_file(self, whitepath):
        self.whitelist.append(whitepath)
        # set()するタイミングでwhitelistの順序は変わる
        self.whitelist = list(set(self.whitelist))
        self.save_fb_file()

    def add_frequent_path_and_save_fb_file(self, frequent_path):
        self.frequent_paths_displayed.append(frequent_path)
        self.frequent_paths_displayed = list(
            set(self.frequent_paths_displayed)
        )
        self.save_fb_file()
