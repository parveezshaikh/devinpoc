from src.filter.FilterComponent import FilterComponent
from src.etl.Component import ComponentInfo


class FilterComponentFactory():
    def getFilterComponent(self,component,spark):
        component = FilterComponent(component,spark)
        return component
