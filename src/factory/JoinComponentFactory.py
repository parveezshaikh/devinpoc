from src.etl.Component import ComponentInfo
from src.join.JoinComponent import JoinComponent

class JoinComponentFactory():
    def getJoinComponent(self,ComponentInfo, spark):
            component = JoinComponent(ComponentInfo,spark)

            return component
