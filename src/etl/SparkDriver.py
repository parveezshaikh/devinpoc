"""
method to get spark Session obbject
"""
from pyspark.sql import  SparkSession
from src.etl.ComponentDriver import ComponentDriver
#from src.etl.CreateDependencyList import CreateDependencyList
from src.util.CheckNode import CheckNode


tempIndependentTaskList = []
tempDependentTaskList = []
enqueuelist = []


class DriverProgram:

    """
    Driver program which runs from spark main to get the required tasks which has the object of each component and
    dependency list to be run in order which needs to be sent to Luigi workflow.
    tasks = dictionary of task id from json and there correspponding object
    tasksid = list of distinct taskid from json
    dependencylist = list of final task id which needs to be run in order in workflpw after a;; the dependency resolution is done
    """

    tasks = {}
    tasksId = []
    dependecylist = []

    @staticmethod
    def getAllConfig(compinfo, spark):
        """
        this is the driver method which will be called from spark main and get all the pre-requisitetaks done before procedding for workflow start.
        :param compinfo:
        :param spark:
        :return:
        """

        DriverProgram.__addTasksToDict(compinfo, spark)
        DriverProgram.__getDistinctTaskId()
        print(f"final task dict ....{DriverProgram.tasks}")
        print(f"final task list..{DriverProgram.tasksId}")
        DriverProgram.__getTaskDependencyId(compinfo,DriverProgram.tasksId,DriverProgram.tasks)
        print(f" final dependency list..{DriverProgram.dependecylist}")

    @staticmethod
    def __addTasksToDict(compinfo, spark):
        """
        method to load taskid and their corresponding object into tasks dictionary
        :param compinfo:
        :param spark:
        :return :
        """
        try:
            DriverProgram.tasks = ComponentDriver(compinfo,spark,DriverProgram.tasks)
        except Exception as e:
            print(f"updating failed in updating tasks ..check component driver class..")
            print(e)

    @staticmethod
    def __getDistinctTaskId():
        try:
            """
            method ot get distinct task id from json into list
            """
            DriverProgram.tasksId = list(DriverProgram.tasks.keys())
        except Exception as e:
            print(f"Failed in getting  tasksId list ..check Spark Driver  class..")
            print(e)

    @staticmethod
    def __getTaskDependencyId(compinfo, taskIdlist,tasks):
        try:
            """
           method to resolve the dependency from taskId list and store it in final dependency list in orde to be run by workflow manager
           :param compinfo: 
           :param taskIdlist: 
           :param tasks: 
           :return dependecylist: 
           """
            print(f"Running to get the depencdency task  in  a ordered list ")
            temp = []
            for compInfo in taskIdlist:
                value = tasks[compInfo]
                if value.fileobj.node_reference is None:
                    CheckNode(tempIndependentTaskList, compInfo)
                    CheckNode(enqueuelist, compInfo)
                    temp.append(compInfo)

            CheckNode(DriverProgram.dependecylist, temp)

            #Reduced Task list
            taskIdlist = [i for i in taskIdlist if i not in tempIndependentTaskList]
            is_last_node = False
            for compInfo in taskIdlist:
                value = tasks[compInfo]
                temp = []
                for node in value.fileobj.node_reference:
                    if node not in enqueuelist:
                        task = tasks[node]
                        tempDependentTaskList.append(node)
                        #enqueuelist.append(node)
                        CheckNode(enqueuelist, node)
                        temp.append(node)
                        is_last_node = False
                    else:
                        is_last_node = True
                if is_last_node:
                    #print(f"is_last_node  ...{is_last_node}")
                    CheckNode(tempDependentTaskList, compInfo)
                    CheckNode(enqueuelist,compInfo)
                    temp.append(compInfo)
                #DriverProgram.dependecylist.append(temp)
                CheckNode(DriverProgram.dependecylist, temp)

            # Reduced Task list
            Pendingtask = [i for i in taskIdlist if i not in tempDependentTaskList]
            if Pendingtask:
                """
                run get dependency task method again if there are still task pending to be added in final dependency list
                """
                DriverProgram.__getTaskDependencyId(compinfo, Pendingtask,tasks)

        except Exception as e:
            print(f"Failed in getting list of dependency task..please check getTaskDependencyId method in SparkDriver class..")
            print(e)




