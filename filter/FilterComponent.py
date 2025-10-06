from src.util.Component import Component
from src.etl.SparkAbstract import SparkAbstract
from src.etl.Component import ComponentInfo

class FilterComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def execute(self):
        try:
            print(f"running for..{self.fileobj.id}")
            reference_node = self.fileobj.node_reference
            final_criteria = self.fileobj.filter_criteria
            #print(final_criteria)
            if SparkAbstract.mapDf:
                filterOutputDF = SparkAbstract.mapDf[reference_node[0]].where(final_criteria[0])

            #filterOutputDF.show(5)

            SparkAbstract.addToDict(self.fileobj.id, filterOutputDF)
            print(f"====> Writing for {self.fileobj.id} Successfully to MapDF ")

        except Exception as e:
            print(f"Filter  dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in FilterComponent-->{e}")


