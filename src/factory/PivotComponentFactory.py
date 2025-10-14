from src.pivot.PivotComponent import PivotComponent


class PivotComponentFactory():
    def getPivotComponent(self, ComponentInfo, spark):
        component = PivotComponent(ComponentInfo, spark)
        return component
