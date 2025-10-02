from src.merge.MergeComponent import MergeComponent

class MergeComponentFactory():
    def getMergeComponent(self,ComponentInfo, spark):
        component = MergeComponent(ComponentInfo,spark)

        return component