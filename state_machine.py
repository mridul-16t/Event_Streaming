from collections import defaultdict

import requests
import json
from pymongo import MongoClient

hosts = defaultdict(list)
vm_state = {}
vm = ""
host = ""
state = ""

client = MongoClient("mongodb://localhost:27017")
db = client['logs']
collection = db['Power_logs']

entry = collection.find({})
print(type(entry))
for document in entry:
    flag = 0
    # print(document.keys())
    key = 'task_status'
    if key in document:
        if document[key] == "Successfully Powered On":
            vm = document['resource_name']
            host = document['parent_resource_name']
            state = "ON"
            flag = 1
        elif document[key] == "Successfully Powered off":
            vm = document['resource_name']
            host = document['parent_resource_name']
            state = "OFF"
            flag = 1
    if flag == 1:
        hosts[host].append(vm)
        vm_state[vm] = state

print(hosts)
print(vm_state)


