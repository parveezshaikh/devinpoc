from src.util import getSpark, Component
from src.etl.Component import ComponentInfo
from src.sort.SortComponent import SortComponent
from src.inputtable.InputTableComponent import InputTableComponent


class InputTableComponentFactory():
    def getInputTableComponent(self,ComponentInfo, spark):
            component = InputTableComponent(ComponentInfo,spark)

            return component