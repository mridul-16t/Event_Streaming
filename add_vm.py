import requests
import json
from pymongo import MongoClient

#GLOBAL VARIABLES
opids_vm = {}
opids_host = {}
store = []
# AUTHENTICATION TOKEN
# headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjFjNTA2M2E1LTgzN2MtNDRiMi1iMWEzLTI1MWQ3OTg4YTE1NyIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiYXpwIjoiY3NwX3ByZF9nYXpfaW50ZXJuYWxfY2xpZW50X2lkIiwiYXV0aG9yaXphdGlvbl9kZXRhaWxzIjpbXSwiZG9tYWluIjoidm13YXJlLmNvbSIsImNvbnRleHQiOiI2Mjg1MzBjYi1kMDZlLTQ5ZWItYTg4Mi1lODY5ZGZhN2RiY2YiLCJwZXJtcyI6WyJleHRlcm5hbC83Y0oybmdTX2hSQ1lfYkliV3VjTTRLV1F3T29fL2xvZy1pbnRlbGxpZ2VuY2U6dXNlciIsImNzcDpvcmdfbWVtYmVyIl0sImV4cCI6MTY1NDY2NzAzMiwiaWF0IjoxNjU0NjY1MjMyLCJqdGkiOiJlWVUzYkdDTkNnV0lOOUdyZmFLdll0Mlo2UkkiLCJhY2N0IjoiZG1vaGFuYXN1bmRhQHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6ImRtb2hhbmFzdW5kYSJ9.qN7zs2R7127XugnxKeaMf3iVIyZvPysdjaVA7ITS5bMeBSvJxcuby1CpAILRtuE06qaU1dM_0hwRp-rM57JiSawKKRzJVHdqCdzXR55uZ02bB4bqEAPw_4Z5FOJ8hjrJr6AKJ7fpncOETODl81sEofaSn55AzhgGQDeFemC2pXF7dg21O-xg1P8Lb5W4kTWxRWqI6J6c7nFYO3S930I9XRgQzrttlLpmNLSIK-hw0UbzrjFinBFz8tRjBw-TUDTz4bNACd2rNkwi6RtwuYkZH7Bf4aG7b3tN3fwWrkFQUIDPWJXf-pV-CRytF0jH5S3U4uPKVTF_Tgz8gG3vWw7ISQ"}
headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjcyZWU1Mzg0LWRlODMtNGI4OC05ZmI3LWFhNTBkMDE1NTgwYSIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiX25vbmNlIjoiMjY5ZTA1ZjAtZGI4MC0xMWVjLThmYzgtNDEwNjAzNWI4OWJiIiwiYXpwIjoib3BzLWFwcC1zdmMtY2ciLCJhdXRob3JpemF0aW9uX2RldGFpbHMiOltdLCJkb21haW4iOiJ2bXdhcmUuY29tIiwiY29udGV4dCI6IjYyODUzMGNiLWQwNmUtNDllYi1hODgyLWU4NjlkZmE3ZGJjZiIsInBlcm1zIjpbImV4dGVybmFsLzdjSjJuZ1NfaFJDWV9iSWJXdWNNNEtXUXdPb18vbG9nLWludGVsbGlnZW5jZTp1c2VyIiwiY3NwOm9yZ19tZW1iZXIiXSwiZXhwIjoxNjU0Njg1MDA3LCJpYXQiOjE2NTQ2NjcwMDgsImp0aSI6IjEwNmZlMmMzLWVkNDItNDUxZi1iMjg1LTZiMTcyNzk2Zjg2ZiIsImFjY3QiOiJtcmlkdWx0QHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6Im1yaWR1bHQifQ.GjHXlxnchAkU2nNLnHpEgSt-KgiVhKJftVfg8kV917Z8SZqOmOQn_HX9dZM_uRBGZp41xWGahWIesYkmS0vt1XnS-S3bFPtJcFAU4leeKZc6UJFEFyJy1JjhuXCbUqjPmDBob0yTIOihnlFCKDVELT06Of_uh86buhHUNS4h1v03PBnhwPPr19tylcbsuNVQBnJijhcVk5tqSmhIBzkv6h7XxPLfy71VKlRzbZOr3x2zNfGM2LPuEG48wpm2bPh-U1oFAuUhgmaGsw2W9ABWXWUttX8p_T48KpAFPcRFNoOteUK2IBFJuG9FlpH5JjMM1YC-ybEh-QzTXoZz9I-9cA"}

data = {
    "namespace": "com.vmware.li",
    "resultLimit": 1000,
    "startTime": 1654497056000,
    "endTime": 1654499753000
}
response = requests.post("https://api.mgmt.cloud.vmware.com/lint/content/alert-instances/query", headers=headers, json=data)
print(response.status_code)
print(response.json())
# Query for Logs
query = {
    "logQuery": "SELECT * FROM logs WHERE sddc_id='e61ffbc7-3888-4b33-a20f-034eb6a9038b' AND (appname='vpxd' OR appname='hostd') AND (component='esxi' OR component='vc') AND (text='createVm' OR text='Creating VM' OR text='Created VM') ORDER BY ingest_timestamp ASC",
    "start": 1654497056000,
    "end": 1654499753000,
    "rows": 50,
    "parallelizationFactor": -1,
    "extractedFieldsEvaluation": {
        "includeAllFields": True,
        "contentPackIdsToInclude": []
    }
}
response1 = requests.post("https://api.mgmt.cloud.vmware.com/ops/query/log-query-tasks", headers=headers, json=query)
print(response1.status_code)
print(response1.json())


response2 = requests.get(f"https://api.mgmt.cloud.vmware.com/lint/query/b3a79550-2544-4f7f-8ca6-0e81a92bbd04", headers=headers)
print(response2.status_code)
print(json.dumps(response2.json(), indent=4))
logs_dict = response2.json()
#print(response2.json())
print("The no of logs extracted are:")

no_of_logs = len(logs_dict['processedResults'])
print(no_of_logs)
order = ["sddc_id", "epoch_timestamp", "timestamp", "hostname", "appname", "component",
         "severity", "operation_type", "message", "resource_name", "resource_type", "parent_resource_name",
         "parent_resource_type",
         "operation_id", "task_status"]


def get_vm_name(text):
    if "Creating VM with spec" in text:
        idx = text.find("name")
        idx1 = text.find(",", idx)
        s1 = text[idx+8:idx1-1]
        return s1
    if "Creating VM" in text:
        idx = text.find("Creating VM")
        s1 = text[idx:]
        l1 = s1.split('/')
        return l1[-1]
    else:
        return ""


# PARSING THE LOGS
for i in range(no_of_logs):
    details = []
    level_1 = logs_dict['processedResults'][i]
    # print(level_1.keys())
    level_2 = level_1['data']
    details.append(level_2['sddc_id'])
    details.append(level_2['log_timestamp'])
    details.append(level_2['timestamp'])
    details.append(level_2['hostname'])
    details.append(level_2['appname'])
    details.append(level_2['component'])
    # Extracting the severity of the log.
    key = 'vmw_esxi_severity'
    if key in level_2:
        details.append(level_2[key])
    else:
        message = level_2['text']
        if "info" in message:
            details.append("info")
        elif "warning" in message:
            details.append("warning")
        elif "error" in message:
            details.append("error")
        elif "verbose" in message:
            details.append("verbose")
    message = level_2['text']

    if "createVm" in message or "Created VM" in message or "Creating VM" in message:
        details.append("VM Creation")
    details.append(level_2['text'])
    key = level_2['vmw_opid']
    idx = key.find(':')
    opid = key[0:idx]

    if opid in opids_vm:
        details.append(opids_vm[opid])
    else:
        vm = get_vm_name(level_2['text'])
        if len(vm) > 0:
            details.append(vm)
            opids_vm[opid] = vm
    details.append("Virtual Machine")
    if level_2['appname'] == "Hostd":
        details.append(level_2['source_hostname'])
        if opid not in opids_host:
            opids_host[opid] = level_2['source_hostname']
    elif 'vmw_vc_managed_host' in level_2:
        details.append(level_2['vmw_vc_managed_host'])
        if opid not in opids_host:
            opids_host[opid] = level_2['vmw_vc_managed_host']
    else:
        if opid in opids_host:
            details.append(opids_host[opid])
        else:
            details.append("")
    details.append("Host")
    key = 'vmw_opid'
    if key in level_2:
        details.append(level_2[key])
    else:
        details.append("")
    key = 'vmw_task_status'
    if key in level_2:
        details.append(level_2[key])
    else:
        details.append("In progress")
    structure = {key: value for key, value in zip(order, details)}
    store.append(structure)

client = MongoClient("mongodb://localhost:27017")
db = client['logs']
collection = db['Add_VM_logs']
for entry in store:
    collection.insert_one(entry)
# for entry in store:
   # print(json.dumps(entry, indent=4))
