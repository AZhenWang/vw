from abc import ABCMeta, abstractmethod


class Interface(object):
    __metaclass__ = ABCMeta  # 指定这是一个抽象类

    @abstractmethod  # 抽象方法
    def run(self):
        pass

