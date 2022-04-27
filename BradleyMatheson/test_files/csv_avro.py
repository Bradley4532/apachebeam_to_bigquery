import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import json
schema = {
    'name': 'avro.example.User',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': ['string','null']},
        {'name': 'id', 'type': ['int','null']},
        {'name': 'salary_in_k', 'type': ['float', 'string','null']},
        {'name': 'phonenumber', 'type': ['string','null']},
        {'name': 'limit', 'type': ['string','null']},
        {'name': 'num', 'type': ['double','long','null']},
        {'name': 'bytes', 'type': ['bytes', 'string','null']},
        {'name': 'time', 'type': ['string','null']},
        {'name': 'geo', 'type': ['string','null']},
        {'name': 'new', 'type': ['boolean', 'string','null']}
        ]
}
schema_parsed = avro.schema.parse(json.dumps(schema))
def generate_avro(filename,numOfIters):
    with open(filename, 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        for i in range(0, numOfIters):
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4, "phonenumber": "44444","limit": "str","num": 9.9E+6,"bytes": "16D6M7PN3w7Cn8mJyrmrUSZY9ummMf5QCGEMuiSmSlw","time": "2019-08-21 12:30:00.45","geo": "POINT(51.500989020415034 -0.12471081312336843)","new": "True"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","limit": "str","num": 9.9E+6,"bytes": "16D6M7PN3w7Cn8mJyrmrUSZY9ummMf5QCGEMuiSmSlw","time": "2019-08-21 12:30:00.45","geo": "POINT(51.500989020415034 -0.12471081312336843)","new": "True"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","limit": "str","num": 9.9E+6,"bytes": "16D6M7PN3w7Cn8mJyrmrUSZY9ummMf5QCGEMuiSmSlw","time": "2019-08-21 12:30:00.45","geo": "POINT(51.500989020415034 -0.12471081312336843)","new": "True"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","limit": "str","num": 9.9E+10,"bytes": "16D6M7PN3w7Cn8mJyrmrUSZY9ummMf5QCGEMuiSmSlw","time": "2019-08-21 12:30:00.45","geo": "POINT(51.500989020415034 -0.12471081312336843)","new": "True"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","limit": "strstrstrstr"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","num": 9.9999999999999999999999999999999999999E+40})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","bytes": "@@@@#####)(*&%$"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","time": "nowtime"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","geo": "here"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444","new": "old"})
            writer.append({"name": "Brad","id": 394,"salary_in_k": 10.4,"phonenumber": "44444"})
            writer.append({"name": "brad"})
        writer.close()

generate_avro(r'C:\\Users\Brad\Documents\code\test_files\test4.avro', 1)