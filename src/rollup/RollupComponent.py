from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract

class RollupComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            node_reference = self.fileobj.node_reference
            rollupGroupByKeys = self.fileobj.rollup_groupby_keys
            rollupAggregation = self.fileobj.rollup_aggregation
            rollupcondition = self.fileobj.rollup_condition

            grp_keys = ",".join(rollupGroupByKeys)
            aggr_keys = ",".join(rollupAggregation)

            """
            Creating temp view for rollup dataframe
            """
            SparkAbstract.mapDf[node_reference[0]].createOrReplaceTempView(self.fileobj.id)

            if not rollupcondition:
                sqlText = f"select {grp_keys},{aggr_keys} from {self.fileobj.id} group by {grp_keys}"

            else:
                sqlText = f"select {grp_keys},{aggr_keys} from {self.fileobj.id} where {rollupcondition}  group by {grp_keys} "

            aggDF = self.spark.sql(sqlText)

            """
             Putting Rollup dataframe back to MapDF for further access
             """

            SparkAbstract.addToDict(self.fileobj.id,aggDF)

            print(f"====> Writing {self.fileobj.id} Successfully to MapDF file")

        except Exception as e:
            print("rollup dataframe operation failed")
            print(e)
            raise Exception(f"Error in RollupComponent --> {e}")



