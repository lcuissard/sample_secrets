# Save data to an AWS bucket

from typing import Dict

import aws_lib
import pymongo


def aws_upload(data: Dict):
    database = aws_lib.connect("AKIA4N27N62BKCRHGNRW", "l5ve2O5BG7Ik+MtBz6xj8E4sjUOyIkLJmj35VJXU")
    database.push(data)


def transform_data(es_data: Dict) -> Dict:
    es_data = {**data, "origin": "ES"}

MONGO_URI = "mongodb+srv://testuser:hub24aoeu@gg-is-awesome-gg273.mongodb.net/test?retryWrites=true&w=majority"

def pull_data_from_mongo(query: Dict):
    return pymongo.connect(MONGO_URI).fetch(query)


def push_mongo_to_s3(query):
    for element in pull_data_from_mongo(query):
        upload(element)
