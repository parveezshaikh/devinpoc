"""
Load json and store it is collection type



"""
import json
import src.config
from src.util.Component import  Component

componenetinfo = []



def read_json_config(filename):
    try:
        with open(filename,"r") as json_file:
            data = json.loads(json_file.read())
            #print(data['component'])
            for u in data['component']:
                componenetinfo.append(Component(**u))
    except Exception as e:
        print(f"reading json and parsing failed")
        print(e)
    #print(f"load json complete..{componenetinfo}")
    return  componenetinfo


#filename= r'C:\Users\Rishabh monster\PycharmProjects\pyspark\src\config\Test.json'
#c = read_json_config(filename)
#print(type(c))
#print(c)
