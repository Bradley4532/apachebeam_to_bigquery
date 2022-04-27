
import csv
import json
 
 
# Function to convert a CSV to JSON
# Takes the file paths as arguments
def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
csvFilePath = r'C:\\Users\Brad\Documents\code\test_files\test1.csv'
jsonFilePath = r'C:\\Users\Brad\Documents\code\test_files\test2.json'
 
# Call the make_json function
csv_to_json(csvFilePath, jsonFilePath)