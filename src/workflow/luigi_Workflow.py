from multiprocessing.spawn import freeze_support

import luigi
from luigi import LocalTarget

from pyspark import SQLContext


import  datetime
from src.etl.SparkDriver import DriverProgram
import os
import sys

class ExecutorTask(luigi.Task):

    taskid = luigi.Parameter()
    filename = luigi.Parameter()
    uniqueId = luigi.Parameter()


    def output(self):
        return luigi.LocalTarget("workflow_output/"+str(self.filename)+"/"+str(datetime.date.today().isoformat() )+"-"+ str(self.uniqueId)+"-"+str(self.taskid)+".csv")

    def run(self):
        try:
            Taskid = self.taskid
            #print(f" Running for ..{Taskid}")
            tasks = DriverProgram.tasks
            objectTorun = tasks[Taskid]
            objectTorun.execute()

            with self.output().open('w') as f:
                f.write("done")
        except Exception as e:
            print(f"task running failed for {Taskid}")
            raise Exception(e)
            sys.exit(1)




class EnqueueTask(luigi.Task):

    filename = luigi.Parameter()
    uniqueId = luigi.Parameter()

    def requires(self):
        #print(f"------------------creating dynamic dependecy to be run in sequence for {filename}----------------------")
        dependency_list = DriverProgram.dependecylist
        print(dependency_list)

        for i in reversed(dependency_list):
            #print(i)
            if len(i) > 1:
                caller = [ExecutorTask(j,self.filename,self.uniqueId) for j in i]
                #print(f"caller from j lop..{caller}")
                yield caller
            elif len(i) == 1:
                caller = ExecutorTask(i[0],self.filename,self.uniqueId)
                #print(f"caller from i lop..{caller}")
                yield caller


    def output(self):
        return luigi.LocalTarget("workflow_output/"+str(self.filename)+"/"+str(datetime.date.today().isoformat() )+"-"+ str(self.uniqueId)+"-"+"enqueueTask"+".csv")


    def run(self):
        with self.output().open('w') as f:
            f.write("done")
        return None
