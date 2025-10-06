from src.rollup.RollupComponent import RollupComponent

class RollupComponentFactory():
    def getRollupComponent(self,component,spark):
        component = RollupComponent(component,spark)
        return component
