from src.util.Component import Component
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import SparkAbstract

class MergeComponent(ComponentInfo):
    def __init__(self, component, spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"Running for..{self.fileobj.id}")
            reference_node = self.fileobj.node_reference
            mergekey = self.fileobj.merge_key

            mergeDF = SparkAbstract.mapDf[reference_node[0]].orderBy(mergekey)

            SparkAbstract.addToDict(self.fileobj.id, mergeDF)
            print(f"====> Writing  {self.fileobj.id} Successfully to MapDF")

        except Exception as e:
            print("Error in Merge Component")
            print(e)
            raise Exception(f"Error in MergeComponent-->{e}")
