

class BaseDetector(object):
    """docstring for BaseDetector"""
    def learn(self, datalist):
        raise NotImplementedError()

    def detect(self, datalist):
        raise NotImplementedError()

    def save_file(self, path):
        raise NotImplementedError()

    def load_file(self, path):
        raise NotImplementedError()
