import luigi

from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import  SparkAbstract
from pyspark.sql import *

class OutputCSVComponent(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def test(self):
        print(f"tesrt passed for {self.fileobj.id} from output")


    def execute(self):
        try:
                print(f"running for..{self.fileobj.id}")
                node_refercne = self.fileobj.node_reference
                #print(node_refercne)
                print(self.fileobj.output_data_file_path)
                if node_refercne[0] in SparkAbstract.mapDf.keys():
                    SparkAbstract.mapDf[node_refercne[0]].coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(self.fileobj.output_data_file_path)
                    SparkAbstract.addToDict(self.fileobj.id, SparkAbstract.mapDf[node_refercne[0]])
                    print(f"====> Writing {self.fileobj.id} Successfully to output file")
                else:
                    raise Exception(f" {node_refercne[0]} doesnt not exist in MAPDF to run OUTPUTCSV")

        except Exception as e:
            print(f"WriteOutput dataframe operation failed  ")
            print(e)
            raise Exception(f"Error in OutputCSVComponent--->{e}")
            sys.exit(1)