"""
This module contains generic code to join tables based on the input json and then stroe the dtaa back to Abstractclass MapDF.
It recieves json object and spark session.
It inherits abstract class Componentinfo and implement execute method.
"""
import luigi

from src.util import  SchemaHandler
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import  SparkAbstract
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType,DateType,FloatType,LongType

class JoinComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def test(self):
        print(f"tesrt passed for {self.fileobj.id} from join")

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            node_refercne = self.fileobj.node_reference
            join_type = self.fileobj.join_type
            join_condition = self.fileobj.join_conditions
            uniquecollist = set()
            collist =[]
            """creating temp view in spark from id column nd also creating set to contain all the distinct
                cols list from both the dataframe
            """
            for joinDF in node_refercne:
                joinDF_tdst = SparkAbstract.mapDf[joinDF]
                cols = joinDF_tdst.columns
                for col in cols:
                    if col not in uniquecollist:
                        collist.append(f'{joinDF}.{col}')
                        uniquecollist.add(col)

                """
                spark temp view in spark for the input dataframes
                """
                SparkAbstract.mapDf[joinDF].createOrReplaceTempView(joinDF)

            """
            string of columns from  the tables spearated with comma 
            """
            colList =",".join(collist)

            """
            creating final select string to be used in spark sql
            """
            tableList = ""
            for index,inputtable in enumerate(node_refercne):
                    if index == 0:
                        tableList += inputtable
                    else:
                        temp_stat = f" {join_type[index-1]} join {inputtable} on ({join_condition[index-1]})"
                       # print(temp_stat)
                        tableList += temp_stat

            sql_text = f"select {colList} from {tableList}"

            """
            final DF after join and storing into SparkAbstract MapDF for further use
            """
            joinDF = self.spark.sql(sql_text)
            #joinDF.show(5)
            SparkAbstract.addToDict(self.fileobj.id, joinDF)
            print(f"====> Writing for {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print(f"join dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in JoinComponent-->{e}")







