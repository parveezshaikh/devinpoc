from src.etl.Component import ComponentInfo
from src.input.InputCSVFileComponent import InputCSVFile

class InputFileComponentFactory():
    def getInputComponent(self,ComponentInfo, spark):
            if ComponentInfo.input_data_file_type == 'CSV':
                component = InputCSVFile(ComponentInfo,spark)

            return component




