from src.util.Component import Component
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import  SparkAbstract


class InputTableComponent(ComponentInfo):
    def __init__(self, component, spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"running for..{self.fileobj.id}")
            input_table_type = self.fileobj.input_table_type
            db_url = self.fileobj.database_url

            ConnectionProp = property()
            ConnectionProp.__set__("user", self.fileobj.db_username)
            ConnectionProp.__set__("password", self.fileobj.db_password)
            # if condition
            tableDF = self.spark.read.jdbc(db_url, self.fileobj.table_name, ConnectionProp)
            # else:
            # tableDF = self.spark.sql(f"select * from {self.fileobj.table_name}")

            SparkAbstract.mapDf(self.fileobj.id, tableDF)

        except Exception as e:
            print("Error in Input Table Component")
            print(e)