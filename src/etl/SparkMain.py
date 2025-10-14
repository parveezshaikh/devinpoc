"""
entry point for spark application
1) start spark session
2)parse json
3) Perform the various operatin present into json
"""
from luigi import LuigiStatusCode

from src.util import getSpark, read_json
import configparser as cp  # load form applicaiton properties
import luigi
from src.etl.SparkDriver import DriverProgram
from src.workflow.luigi_Workflow import EnqueueTask
import uuid
import sys
import os


class SparkMain():

    def __init__(self, json_config):
        self.json_filename = json_config

    spark = getSpark.get_spark_session()

    def load_json(self):
        return read_json.read_json_config(self.json_filename)


"""
entry point for spark application
"""
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Error..Json filename is not passed")
        sys.exit(-1)

    unique_id = uuid.uuid4()
    config = cp.ConfigParser()
    config.read('../application.properties')
    filepath = config.get('basicdetail', 'jsonfilepath')

    filename = str(sys.argv[1])
    print(f"filename passed is {filename}")

    # If user passed an absolute path, use it directly. Otherwise build path from properties.
    if os.path.isabs(filename):
        fulfilename = filename
    else:
        # filepath in properties is expected to be relative to src/etl when running from that folder
        fulfilename = os.path.normpath(os.path.join(filepath, filename))

    print(f"fullfilename ...{fulfilename}")
    main_init = SparkMain(fulfilename)
    # load the json as class object
    componenetinfo = main_init.load_json()

    print(f"Spark process starting for filename {filename} and {unique_id}")

    # get the spark session
    spark = getSpark.get_spark_session()
    # Creating the final dependent task list
    DriverProgram.getAllConfig(componenetinfo, spark)

    print("Calling the luigi workflow pipeline to run the task in order")
    """try:
        luigi_run_result = luigi.build([EnqueueTask(filename,unique_id)], workers=1, local_scheduler=True, detailed_summary=True)
        status = luigi_run_result.status
        if status == LuigiStatusCode.FAILED:
            raise Exception("Luigi Pipeline  failed")
        else:
            print(f"Luigi Pipeline is successful for json {filename}")

    except Exception as e:
        print(f"Error occured while Running pipeline for json {filename}--> {e}  ")
        sys.exit(1)
    """



