import luigi

from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import  SparkAbstract

class SortComponent(ComponentInfo):
    def __init__(self, component, spark):
        self.fileobj = component
        self.spark = spark

    def test(self):
        print(f"tesrt passed for {self.fileobj.id} from sort")

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            node_reference = self.fileobj.node_reference
            sort_col      = self.fileobj.sorting_column_name

            """ creating string of columns for the order by """
            orderKeys = ",".join(sort_col)

            """
            Final sql text to be used in spark sql
            """
            sqltext = f"select * from {node_reference[0]} order by  {orderKeys}"
            """
            creating temp view in spark for the join dataframe
            """
            SparkAbstract.mapDf[node_reference[0]].createOrReplaceTempView(node_reference[0])

            sortOutputDF = self.spark.sql(sqltext)
            #sortOutputDF.show(10)
            SparkAbstract.addToDict(self.fileobj.id, sortOutputDF)

            print(f"====> Writing  {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print(f"sort dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in SortComponent--> {e}")
        """
        putting Sort dataframe back to MapDF for further access
       """


