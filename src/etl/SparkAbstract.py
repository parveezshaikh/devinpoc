"""
Abstract class to capture application level properties
Static Method to add values in MapDF varibale to be used across application
"""



class SparkAbstract:

    mapDf = dict()


    def __init__(self):
        pass
    """
    This method checks if id and its value is present into dictionary.
    If not - then add it
    If yes - update it with new value
    """
    @staticmethod
    def addToDict(key,value):
        try:
            print(f"adding in mapddf for {key}")
            if key in SparkAbstract.mapDf:
               SparkAbstract.mapDf.update({key: value})
            else:
             SparkAbstract.mapDf[key] = value

            #print(SparkAbstract.mapDF[key])
        except Exception as e:
            print(f"updating failed in MApdf for {key} ")
            print(e)




