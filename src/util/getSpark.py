"""
method to get spark Session obbject
"""
from pyspark.sql import  SparkSession

def get_spark_session():
        """
        Create spark session if session doesn't exist
        :param appname:
        :return:spark session
        """
        try:
            return (
                SparkSession
                    .builder
                    .master("local[*]")
                    .appName("pysparkpoc")
                    .enableHiveSupport()
                    .getOrCreate()
            )
        except RuntimeError:
            raise RuntimeError("Spark Context not available")


