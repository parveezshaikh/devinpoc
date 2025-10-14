from src.etl.Component import ComponentInfo
from src.util import Component

from src.join.JoinComponent import JoinComponent
from src.sort.SortComponent import SortComponent
from src.output.OutputCSVComponent import OutputCSVComponent
from src.input.InputCSVFileComponent import InputCSVFile
from src.factory import InputFileComponentFactory,JoinComponentFactory,OutputFileComponentFactory,SortComponentFactory,FilterComponentFactory,InputTableComponentFactory,RollupComponentFactory,DedupSortComponentFactory,ScanComponentFactory,LookupComponentFactory,MergeComponentFactory,PartitionComponentFactory,PivotComponentFactory


def ComponentDriver(ComponentInfo,spark,tasks):
        try:
            #tasks ={}
            for ComponentInfo in ComponentInfo:
                if (ComponentInfo.component_type).upper() == 'INPUTFILE':
                    component = InputFileComponentFactory.InputFileComponentFactory().getInputComponent(ComponentInfo, spark)
                    #print(f"checkign the object type..{isinstance(component, InputCSVFile)}")
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'JOIN':
                    component = JoinComponentFactory.JoinComponentFactory().getJoinComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'SORT':
                    component = SortComponentFactory.SortComponentFactory().getSortComponent(ComponentInfo,spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'OUTPUTFILE':
                    component = OutputFileComponentFactory.OutputFileComponentFactory().getOutputComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'FILTER':
                    component = FilterComponentFactory.FilterComponentFactory().getFilterComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'INPUTTABLE':
                    component = InputTableComponentFactory.InputTableComponentFactory().getInputTableComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'ROLLUP':
                    component = RollupComponentFactory.RollupComponentFactory().getRollupComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'DEDUPSORT':
                    component = DedupSortComponentFactory.DedupSortComponentFactory().getDedupSortComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'LOOKUP':
                    component = LookupComponentFactory.LookupComponentFactory().getLookupComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'SCAN':
                    component = ScanComponentFactory.ScanComponentFactory().getScanComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'MERGE':
                    component = MergeComponentFactory.MergeComponentFactory().getMergeComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'PARTITION':
                    component = PartitionComponentFactory.PartitionComponentFactory().getPartitionComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
                elif (ComponentInfo.component_type).upper() == 'PIVOT':
                    component = PivotComponentFactory.PivotComponentFactory().getPivotComponent(ComponentInfo, spark)
                    tasks[ComponentInfo.id] = component
            #print(tasks)
            return tasks

        except Exception as e:
            print(f'failed in updating tasks for  --->{ComponentInfo.id} in Component driver')
            print(e)


