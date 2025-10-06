#!/usr/bin/env python3
"""
Small runner to execute the pipeline components in dependency order without Luigi.
Usage: python run_pipeline.py <path-to-json>
If no argument is provided it defaults to ../config/local_test3.json
"""
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.util import getSpark, read_json
from src.etl.SparkDriver import DriverProgram


def main(json_path=None):
    if not json_path:
        json_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../config/local_test3.json'))

    json_path = os.path.normpath(json_path)
    print(f"Using JSON config: {json_path}")

    spark = getSpark.get_spark_session()

    compinfo = read_json.read_json_config(json_path)

    # Build tasks and dependency list
    DriverProgram.getAllConfig(compinfo, spark)

    print(f"Resolved dependency list: {DriverProgram.dependecylist}")

    # Execute in dependency order
    for group in DriverProgram.dependecylist:
        # group can be a list of task ids to run in parallel — we run sequentially here
        for taskid in group:
            print(f"Executing task: {taskid}")
            task_obj = DriverProgram.tasks.get(taskid)
            if not task_obj:
                print(f"  Task {taskid} not found in DriverProgram.tasks")
                continue
            try:
                task_obj.execute()
            except Exception as e:
                print(f"  Task {taskid} failed: {e}")
                raise

    print("Pipeline run complete.")


if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
