
"""
Method to check node to add in dependency list is already there and not null,
add it in dependency list
"""
def CheckNode(listotadd,nodevalue):
    if nodevalue not in listotadd and nodevalue :
        listotadd.append(nodevalue)

    return listotadd

