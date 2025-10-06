from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys


class DedupSortComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"running for..{self.fileobj.id}")
            node_reference = self.fileobj.node_reference
            dedupDFColumnsList  =self.fileobj.dedup_sort_columns
            inputKeys = ",".join(dedupDFColumnsList)
            dedupsortDF = SparkAbstract.mapDf[node_reference[0]]

            if SparkAbstract.mapDf and dedupsortDF:
                    dedupType = self.fileobj.dedup_sort_type
                    column_list =[F.col(x) for x in dedupDFColumnsList]

                    if dedupType.upper() == 'FIRST':
                        firstWindowType = Window.partitionBy(column_list).orderBy("SeqNumber")
                        dedupsortDF = dedupsortDF.orderBy(dedupDFColumnsList) \
                            .withColumn("SeqNumber", F.monotonically_increasing_id()) \
                            .withColumn("rn", F.row_number().over(firstWindowType)) \
                            .where(F.col("rn") == 1) \
                            .drop("rn", "SeqNumber")

                    elif dedupType.upper() == 'LAST':
                        lastWindowType = Window.partitionBy(column_list).orderBy("SeqNumber",ascending=False)
                        dedupsortDF = dedupsortDF.orderBy(dedupDFColumnsList) \
                            .withColumn("SeqNumber", F.monotonically_increasing_id()) \
                            .withColumn("rn", F.row_number().over(lastWindowType)) \
                            .where(F.col("rn") == 1) \
                            .drop("rn", "SeqNumber")

                    elif dedupType.upper() == 'UNIQUE':
                        uniqueWindowType = Window.partitionBy(column_list).orderBy("SeqNumber", ascending=False)
                        dedupsortDF.orderBy(dedupDFColumnsList) \
                            .withColumn("SeqNumber", F.monotonically_increasing_id()) \
                            .withColumn("rn", F.row_number().over(uniqueWindowType)) \
                            .createOrReplaceTempView(self.fileobj.id)

                        sqltext = f"SELECT * FROM (SELECT *,MAX(rn) OVER (PARTITION BY {inputKeys}) as maxB from {self.fileobj.id} )  {self.fileobj.id} WHERE rn=maxB"
                        dedupsortDF= self.spark.sql(sqltext).filter("rn<2").drop("rn","SeqNumber","maxB").orderBy(dedupDFColumnsList)

                    elif dedupType.upper() == 'UNIQUEALL':
                        dedupsortDF = dedupsortDF.distinct()
            """
            Putting DedupDF dataframe back to MapDF for further access
            """
            dedupsortDF.show()
            SparkAbstract.addToDict(self.fileobj.id, dedupsortDF)

            print(f"====> Writing for {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print(f"DEDUPSORT dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in DEDEUPSORT--->{e}")
            sys.exit(1)




