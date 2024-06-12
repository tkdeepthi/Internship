import os
import json
 
# List of keys to convert to lowercase
keys_to_convert = ["Version", "SubscriptionId", "Token","Events","EventCategory", "SequenceNumber", "DateTime", "EventDetailType","EventDetail", "Links","Rel","Href"]
 
 
def convert_first_letter_lower(string):
    """Converts the first letter of a string to lowercase."""
    if string:
        return string[0].lower() + string[1:]
    else:
        return string
 
def convert_keys_to_lower(json_data, keys_to_convert):
    """Converts the first letter of keys in JSON data to lowercase."""
    if isinstance(json_data, dict):
        new_data = {}
        for key, value in json_data.items():
            new_key = convert_first_letter_lower(key) if key in keys_to_convert else key
            new_data[new_key] = convert_keys_to_lower(value, keys_to_convert)
        return new_data
    # here checking list format of keys like Events
    elif isinstance(json_data, list):
        return [convert_keys_to_lower(item, keys_to_convert) for item in json_data]
    else:
        return json_data
 
 
exceptionDataList = []
noOriginatorList = []
originatorList = []
allFilesList = []
notJsonFile = []
corruptDataLoop = []
 
totalNumberOfObjects = 0
totalDataPacketsAppended = 0
 
# Get a list of all files that match the pattern
# Get a list of all files that match the pattern in the "convert" directory
input_path = "C:/Users/TaKi386/Desktop/Feature_usage_new/source"
listOfFolder = os.listdir(input_path)
 
 
for folder in listOfFolder:
    folderPath = input_path + "/" + folder + "/"
 
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(folderPath):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
        print("List of Files",listOfFiles)
 
 
    newList = []
    for item in listOfFiles:
        if item.endswith('.json'):
            newList.append(item)
        else:
            notJsonFile.append(item)
   
    print("Number of Objects", len(newList))
    totalNumberOfObjects = totalNumberOfObjects + len(newList)
 
    exceptionDataListLoop = []
    noOriginatorListLoop = []
    originatorListLoop = []
    corruptDataList = []
    dataList = []
    # output_data = []
    i=0
    # Iterate over the matching files
    for fPath in newList:
        print("Working", i)
        i = i+1
        try:
            jFile = open(fPath)
            input_data = json.load(jFile)
        except:
            corruptDataLoop.append(fPath)
            print(fPath)
            continue
       
        # Convert the specified keys to lowercase
        output_data = convert_keys_to_lower(input_data, keys_to_convert)
 
        try:
            a = output_data["Data"]["events"]
        except :
            exceptionDataListLoop.append(fPath)
            continue
 
        try:
            a = output_data['originator']['originatorDetail']
        except :
            noOriginatorListLoop.append(fPath)
            continue
       
       
 
        if output_data['originator']['originatorDetail']:
            originatorListLoop.append(fPath)
            kappa = {'platform': folder,'filePath':fPath,'data' : output_data}
            dataList.append(kappa)
       
   
    totalDataPacketsAppended = totalDataPacketsAppended + len(dataList)
    output_path = "C:/Users/TaKi386/Desktop/Feature_usage_new/after_condensing/{}_condense.json".format(folder)
    # Write the modified data back to the file
    with open(output_path, 'w') as outfile:
        json.dump(dataList, outfile)
        print("FileWritten")
   
    corruptDataList.extend(corruptDataLoop)
    exceptionDataList.extend(exceptionDataListLoop)
    originatorList.extend(originatorListLoop)
    noOriginatorList.extend(noOriginatorListLoop)
    allFilesList.extend(listOfFiles)
 
 
print("Total Number Of files : ", len(allFilesList))
 
print("Total Number Of Json files : ", totalNumberOfObjects)
 
print("Total number of Non Json files :",len(notJsonFile))
 
print("Files processed: ", totalDataPacketsAppended)
 
print("Number of files without proper data & events:", len(exceptionDataList))
 
print("Number of files without originator:",len(noOriginatorList))
 
print("Number of files with null or missing braces:",len(corruptDataList))