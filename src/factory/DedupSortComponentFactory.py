from src.dedupsort.DedupSortComponent import DedupSortComponent


class DedupSortComponentFactory():
    def getDedupSortComponent(self,component,spark):
        component = DedupSortComponent(component,spark)
        return component
