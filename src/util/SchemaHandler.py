import pyspark.sql.types   as data_types
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType,DateType,FloatType,LongType



def get_schema(inputfile):
    schema_entries = list()
    try:
        with open(inputfile,"r") as file:
            for line in file:
                #print(line.split(":")[0])
                #print(line.split(":")[1])

                field_type = __spark_datatype(str(line.split(":")[1]).strip())
                c1 = data_types.StructField(str(line.split(":")[0]).strip(),field_type)
                schema_entries.append(c1)
    except IOError   as e:
        print(f"creating struct schema failed for {inputfile} ")
        print(e)

    return data_types.StructType(schema_entries)

def __spark_datatype(fieldtype):
    if  (fieldtype == 'DATE'):
        return data_types.DateType()
    elif (fieldtype == 'DOUBLE'):
        return data_types.DoubleType()
    elif (fieldtype == 'INT'):
        return data_types.IntegerType()
    elif (fieldtype.upper() == "TIMESTAMP"):
        return  data_types.TimestampType
    elif (fieldtype.upper() == 'LONG'):
        return data_types.LongType()
    elif (fieldtype.upper() == 'FLOAT'):
        return data_types.FloatType()
    elif (fieldtype.upper().strip() == 'STRING'):
        return data_types.StringType()
    else:
        print("datatype not found")



