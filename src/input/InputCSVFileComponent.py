"""
Generic component to load input CSV file and put the dtaarframe into SparkAbstract mapDF for further access
It recieves json object and spark session.
It inherits abstract class Componentinfo and implement execute method.
"""
from src.util import  SchemaHandler
from src.etl.Component import ComponentInfo
from src.etl.SparkAbstract import  SparkAbstract
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType,DateType,FloatType,LongType


class InputCSVFile(ComponentInfo):
    def __init__(self,component,spark):
        self.fileobj = component
        self.spark = spark

    def test(self):
        print(f"tesrt passed for {self.fileobj.id} from input")

    def execute(self):
            try:
                print(f"Running for..{self.fileobj.id}")
                if not self.fileobj.input_schema_file_path :
                    Dataset = self.spark.read.format("csv") \
                                  .option("header",self.fileobj.header) \
                                  .option("inferSchema", "true") \
                                  .option("quote", "\"").option("quoteMode", "NON_NUMERIC") \
                                  .option("delimiter",self.fileobj.delimiter) \
                                  .option("dateFormat", "yyyyMMdd") \
                                  .load(self.fileobj.input_data_file_path)
                else:
                    print(f"Running for..{self.fileobj.id}")
                    Dataset = self.spark.read.format("csv") \
                            .option("header", self.fileobj.header) \
                            .option("inferSchema", "false") \
                            .option("quote", "\"").option("quoteMode", "NON_NUMERIC") \
                            .option("delimiter", self.fileobj.delimiter) \
                            .option("dateFormat", "yyyyMMdd") \
                            .schema(SchemaHandler.get_schema(self.fileobj.input_schema_file_path)) \
                            .load(self.fileobj.input_data_file_path)
                """
                    putting input  dataframe back to MapDF for further access
                    """
                # SparkAbstract.addToDict(self.fileobj.id,Dataset)
                SparkAbstract.addToDict(self.fileobj.id, Dataset)
                # Dataset.show(5)

                print(f"====> Writing  {self.fileobj.id} Successfully to MapDF")

            except Exception as e:
                print(f"retriving data from file failed for {self.fileobj.id} ")
                print(str(e))
                raise Exception(f"Error in InputCSVFile--->{e}")





