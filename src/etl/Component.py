from abc import ABC, abstractmethod
from pyspark.sql import  SparkSession


class ComponentInfo(ABC):

    @abstractmethod
    def execute(self):
        pass


