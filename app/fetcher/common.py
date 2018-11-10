from abc import ABCMeta, abstractmethod


class Interface(object):
    __metaclass__ = ABCMeta  # 指定这是一个抽象类

    @abstractmethod  # 抽象方法
    def query(self, api):
        pass

    @staticmethod
    def str2hump(text):
        arr = filter(None, text.lower().split('_'))
        res = ''
        for i in arr:
            res = res + i[0].upper() + i[1:]
        return res
