from src.lookup.LookupComponent import LookupComponent

class LookupComponentFactory():
    def getLookupComponent(self, ComponentInfo, spark):
        component = LookupComponent(ComponentInfo,spark)

        return component