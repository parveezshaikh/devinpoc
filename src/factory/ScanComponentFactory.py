from src.scan.ScanComponent import ScanComponent


class ScanComponentFactory():
    def getScanComponent(self,ComponentInfo,spark):
        component = ScanComponent(ComponentInfo,spark)
        return component