from src.util.Component import Component
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract
from pyspark.sql import functions

class LookupComponent(ComponentInfo):
    def __init__(self,component, spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            reference_node = self.fileobj.node_reference
            lookuptype = self.fileobj.lookup_type[0]
            lookupSelectColumnsKey1 = self.fileobj.lookup_select_key1
            lookupSelectColumnsKey2 = self.fileobj.lookup_select_key2
            lookupJoinSelectColumns1 = self.fileobj.lookup_join_select_columns
            lookupJoinKey = self.fileobj.lookup_join_columns
            lookupSelectColumns = self.fileobj.lookup_select_key

            lookupSelectColumns1 = ",".join(lookupSelectColumnsKey1)
            lookupSelectColumns2 = ",".join(lookupSelectColumnsKey2)
            lookupJoinSelectColumns = ",".join(lookupJoinSelectColumns1)


            if lookuptype.upper() == "SELECTJOIN":
                lookupDF1 = SparkAbstract.mapDf[reference_node[0]].selectExpr(lookupSelectColumns1)
                lookupDF2 = SparkAbstract.mapDf[reference_node[1]].selectExpr(lookupSelectColumns2)
                """
                joining lookup DF1 and DF2
                """
                lookupJoinDF = lookupDF1.join(lookupDF2, lookupDF2[lookupJoinKey[1]]==lookupDF1[lookupJoinKey[0]])
                print("Join DF-----", lookupJoinDF)
                selectDF = lookupJoinDF.selectExpr(lookupJoinSelectColumns)
                print(selectDF)

            else :
                lookupSelectColumnsstr = ",".join(lookupSelectColumns)
                selectDF = SparkAbstract.mapDf[reference_node[0]].selectExpr(lookupSelectColumnsstr)

            SparkAbstract.addToDict(self.fileobj.id,selectDF)
            print(f"====> Writing  {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print("Error in lookup file component")
            print(e)
            raise Exception(f"Error in LookupComponent-->{e}")