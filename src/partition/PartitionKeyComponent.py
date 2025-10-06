from src.util.Component import Component
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract

class PartitionKeyComponent(ComponentInfo):
    def __init__(self, component, spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            reference_node = self.fileobj.node_reference
            partition_key =self.fileobj.partition_key
            ordr_by_cols_list =SparkAbstract.mapDf[reference_node[0]].columns
            order_by_cols = ",".join(ordr_by_cols_list)
            SparkAbstract.mapDf[reference_node[0]].createOrReplaceTempView("part_tb")

            query = f"select *,ROW_NUMBER() OVER (PARTITION BY {partition_key}  order by {order_by_cols} ) as partBy from part_tb"

            partitionKeyDF = self.spark.sql(query)
            #partitionKeyDF.show()

            partitionKeyDF = partitionKeyDF.drop("partBy")

            SparkAbstract.addToDict(self.fileobj.id, partitionKeyDF)

            partitionKeyDF.show()

            print(f"====> Writing  {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print("Error in Partition Component")
            print(e)
            raise Exception(f"Error in PartitionKeyComponent-->{e}")
