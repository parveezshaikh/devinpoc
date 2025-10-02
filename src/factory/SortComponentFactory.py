from src.util import getSpark, Component
from src.etl.Component import ComponentInfo
from src.sort.SortComponent import SortComponent


class SortComponentFactory():
    def getSortComponent(self,ComponentInfo, spark):
            component = SortComponent(ComponentInfo,spark)

            return component
