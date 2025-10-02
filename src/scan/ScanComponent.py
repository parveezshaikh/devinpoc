from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract
from pyspark.sql import functions as F

class ScanComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            node_ref = self.fileobj.node_reference
            ScanDFColumnsPartitionKey = self.fileobj.scan_partition_key
            ScanDFColumnsOrderBy = self.fileobj.scan_order_by_column
            Scansumcolumn=self.fileobj.scan_sum_column

            scanDFColumnsSet = set()
            for col1 in ScanDFColumnsOrderBy:
                scanDFColumnsSet.add(col1) #split(" ")[0])

            for col2 in ScanDFColumnsPartitionKey:
                scanDFColumnsSet.add(col2)

            orderByColumn = ",".join(ScanDFColumnsOrderBy)
            partitionKeyColumn = ",".join(ScanDFColumnsPartitionKey)
            allColumns = ",".join(scanDFColumnsSet)

            """
            Creating temp view for Scan Component
            """
            SparkAbstract.mapDf[node_ref[0]].createOrReplaceTempView(self.fileobj.id)

            query = f"select {allColumns},sum({Scansumcolumn}) OVER(PARTITION BY {partitionKeyColumn}  order by {orderByColumn} )as Cumulative_Sum from {self.fileobj.id} order by {orderByColumn},Cumulative_Sum"

            scanDF = self.spark.sql(query)

            scanDF.createOrReplaceTempView(self.fileobj.id)

            withIndexDF = self.spark.sql(f"select *, ROW_NUMBER() over(order by {orderByColumn}) as Index from {self.fileobj.id}")

            withIndexDF = withIndexDF.drop("Index")

            SparkAbstract.addToDict(self.fileobj.id,withIndexDF)

            print(f"====> Writing for {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print(f"DEDUPSORT dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in DEDEUPSORT--->{e}")
            sys.exit(1)