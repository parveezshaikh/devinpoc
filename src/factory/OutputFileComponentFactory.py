from src.etl.Component import ComponentInfo
from src.output.OutputCSVComponent import OutputCSVComponent

class OutputFileComponentFactory():
    def getOutputComponent(self,ComponentInfo, spark):
        if ComponentInfo.output_data_file_type == 'CSV':
            component = OutputCSVComponent(ComponentInfo, spark)

        return component


