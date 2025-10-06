from src.partition.PartitionKeyComponent import PartitionKeyComponent

class PartitionComponentFactory():
    def getPartitionComponent(self,ComponentInfo, spark):
        component = PartitionKeyComponent(ComponentInfo,spark)

        return component