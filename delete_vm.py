import requests
import json
from pymongo import MongoClient
# opids_vm = {}
# opids_host = {}
store = []
# version = ""
# cpus = ""
# headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjcyZWU1Mzg0LWRlODMtNGI4OC05ZmI3LWFhNTBkMDE1NTgwYSIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiX25vbmNlIjoiMjY5ZTA1ZjAtZGI4MC0xMWVjLThmYzgtNDEwNjAzNWI4OWJiIiwiYXpwIjoib3BzLWFwcC1zdmMtY2ciLCJhdXRob3JpemF0aW9uX2RldGFpbHMiOltdLCJkb21haW4iOiJ2bXdhcmUuY29tIiwiY29udGV4dCI6IjYyODUzMGNiLWQwNmUtNDllYi1hODgyLWU4NjlkZmE3ZGJjZiIsInBlcm1zIjpbImV4dGVybmFsLzdjSjJuZ1NfaFJDWV9iSWJXdWNNNEtXUXdPb18vbG9nLWludGVsbGlnZW5jZTp1c2VyIiwiY3NwOm9yZ19tZW1iZXIiXSwiZXhwIjoxNjU0NTE2ODU1LCJpYXQiOjE2NTQ0OTg4NTUsImp0aSI6IjZiMDUyZWRjLTZlOWMtNGM5Yi1hY2U5LTg0YTFkOTE5NmY0MCIsImFjY3QiOiJtcmlkdWx0QHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6Im1yaWR1bHQifQ.pxIX5WtPNjsF52v5O5fz3o6NfEHTSkv_L5OmCs61gdgSTQqKjjPl3tahOsX24wSXuaw4J7NXJlSuSNF8ClsXhkwa8Od0steg5MXDRG-E4qiuAk4ABV03cSo6EhTIrsG_eXnyBh_Qfw6Rpsx3Zp4HXJP1wdmtGCzxS1VW1kisFxuREpLFbj6EmESCgCP74FAjyudNgIwgpR_FFxoKEDQ9uGLCMvKxPBWyRNSHm8FSgcP3EEtVsWhlojlSjaqQu8nBkC9ZnhnnNHDCJBi2vG_wcdy1r8bsPNQKj37APbyHdRVWUaZYrJtbIkxLweNtyT3NzOwKJGFUMozOsF9nO2ERsg"}
# headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjcyZWU1Mzg0LWRlODMtNGI4OC05ZmI3LWFhNTBkMDE1NTgwYSIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiX25vbmNlIjoiMjY5ZTA1ZjAtZGI4MC0xMWVjLThmYzgtNDEwNjAzNWI4OWJiIiwiYXpwIjoib3BzLWFwcC1zdmMtY2ciLCJhdXRob3JpemF0aW9uX2RldGFpbHMiOltdLCJkb21haW4iOiJ2bXdhcmUuY29tIiwiY29udGV4dCI6IjYyODUzMGNiLWQwNmUtNDllYi1hODgyLWU4NjlkZmE3ZGJjZiIsInBlcm1zIjpbImV4dGVybmFsLzdjSjJuZ1NfaFJDWV9iSWJXdWNNNEtXUXdPb18vbG9nLWludGVsbGlnZW5jZTp1c2VyIiwiY3NwOm9yZ19tZW1iZXIiXSwiZXhwIjoxNjU0NTQ4OTQzLCJpYXQiOjE2NTQ1MzA5NDMsImp0aSI6IjQ3NzI2NmIwLTg4OWItNDZiZS04Y2MwLTEyMWE1MjIzYmNkMCIsImFjY3QiOiJtcmlkdWx0QHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6Im1yaWR1bHQifQ.eV6t30M0v8EDOHLwLC2G7oyvwXSOVgBWZpANPa7D5fr0pTsen2qELNy8vnNwb7nDRV9jYt332Ldrfq3T6kKnQqAf0b49iCQAc1Rt1kucdZr_q9AiJBjgZRRQqtQcLTK7gOM4AGPn-kkJ-W4Uev2VsNQF5dS7fWs6Whb_w_QGlZgSx_IlnoZfDPIJu0m3QHsv9mf2Uo2jHek7PVh12DBdlHf9E55NtbF8OFbSk13OnHyJivyoTxm8K_gXmT6j8ghj4jiAscWc3EZnf3LKK3LXSFGkqmD_TAZgc99tANEH64w7E0YjBFx5DubNYJCr5OEEsoTN4bubaqvY9HAi_jQF1A"}
# headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjcyZWU1Mzg0LWRlODMtNGI4OC05ZmI3LWFhNTBkMDE1NTgwYSIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiX25vbmNlIjoiMjY5ZTA1ZjAtZGI4MC0xMWVjLThmYzgtNDEwNjAzNWI4OWJiIiwiYXpwIjoib3BzLWFwcC1zdmMtY2ciLCJhdXRob3JpemF0aW9uX2RldGFpbHMiOltdLCJkb21haW4iOiJ2bXdhcmUuY29tIiwiY29udGV4dCI6IjYyODUzMGNiLWQwNmUtNDllYi1hODgyLWU4NjlkZmE3ZGJjZiIsInBlcm1zIjpbImV4dGVybmFsLzdjSjJuZ1NfaFJDWV9iSWJXdWNNNEtXUXdPb18vbG9nLWludGVsbGlnZW5jZTp1c2VyIiwiY3NwOm9yZ19tZW1iZXIiXSwiZXhwIjoxNjU0NTg2OTAzLCJpYXQiOjE2NTQ1Njg5MDMsImp0aSI6IjdmM2JmNmZjLTNkZmUtNDAxMi1iMzQwLTM1MGU1Yzc5ZThmZSIsImFjY3QiOiJtcmlkdWx0QHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6Im1yaWR1bHQifQ.EQVDB_BXKqcLJG5cUnorIjfUx9XNpaKTuNgPqkeyEz5V_HeOa7NFqbLN1dwgC3CI19zuaUPeXb3VrAvjdSZAQKGg9hczypX8OZaKZnOLbOkRX8A3H-DVndE9gTsI2WjlDBBF8mZq-IBJrvQWtGayCW4zAhDZ5xollT7nBLSPt9N188mnQbVI8os7pgj47NW4-KBHaeOfdYNM2PLL__ufW7m8NJSSuXRYCc8O0VaDqN4NBgt66s6bTqKVbX6snwlfSdwI-nBHjMBtITg1NQ6ggu0IRmIuD1p0GEaT51Fovy-vFM2B7HyCgM9d_DhOuvSSWP47Ycnq1CpcWkh6o9cmlw"}
headers = {"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNpZ25pbmdfMiJ9.eyJzdWIiOiJ2bXdhcmUuY29tOjcyZWU1Mzg0LWRlODMtNGI4OC05ZmI3LWFhNTBkMDE1NTgwYSIsImlzcyI6Imh0dHBzOi8vZ2F6LmNzcC12aWRtLXByb2QuY29tIiwiY29udGV4dF9uYW1lIjoiZGE4Y2RjMTAtZmI2NS00YjgwLTgzMGYtNzdlNmUzYmI4YTE5IiwiX25vbmNlIjoiMjY5ZTA1ZjAtZGI4MC0xMWVjLThmYzgtNDEwNjAzNWI4OWJiIiwiYXpwIjoib3BzLWFwcC1zdmMtY2ciLCJhdXRob3JpemF0aW9uX2RldGFpbHMiOltdLCJkb21haW4iOiJ2bXdhcmUuY29tIiwiY29udGV4dCI6IjYyODUzMGNiLWQwNmUtNDllYi1hODgyLWU4NjlkZmE3ZGJjZiIsInBlcm1zIjpbImV4dGVybmFsLzdjSjJuZ1NfaFJDWV9iSWJXdWNNNEtXUXdPb18vbG9nLWludGVsbGlnZW5jZTp1c2VyIiwiY3NwOm9yZ19tZW1iZXIiXSwiZXhwIjoxNjU0Njg1MDA3LCJpYXQiOjE2NTQ2NjcwMDgsImp0aSI6IjEwNmZlMmMzLWVkNDItNDUxZi1iMjg1LTZiMTcyNzk2Zjg2ZiIsImFjY3QiOiJtcmlkdWx0QHZtd2FyZS5jb20iLCJ1c2VybmFtZSI6Im1yaWR1bHQifQ.GjHXlxnchAkU2nNLnHpEgSt-KgiVhKJftVfg8kV917Z8SZqOmOQn_HX9dZM_uRBGZp41xWGahWIesYkmS0vt1XnS-S3bFPtJcFAU4leeKZc6UJFEFyJy1JjhuXCbUqjPmDBob0yTIOihnlFCKDVELT06Of_uh86buhHUNS4h1v03PBnhwPPr19tylcbsuNVQBnJijhcVk5tqSmhIBzkv6h7XxPLfy71VKlRzbZOr3x2zNfGM2LPuEG48wpm2bPh-U1oFAuUhgmaGsw2W9ABWXWUttX8p_T48KpAFPcRFNoOteUK2IBFJuG9FlpH5JjMM1YC-ybEh-QzTXoZz9I-9cA"}

data = {
    "namespace": "com.vmware.li",
    "resultLimit": 1000,
    "startTime": 1654576256000,
    "endTime": 1654583066000
}
response = requests.post("https://api.mgmt.cloud.vmware.com/lint/content/alert-instances/query", headers=headers, json=data)
print(response.status_code)
print(response.json())

query = {
    "logQuery": "SELECT * FROM logs WHERE sddc_id='e61ffbc7-3888-4b33-a20f-034eb6a9038b' AND (appname='vpxd' OR appname='hostd') AND (component='esxi' OR component='vc') AND (text='Virtual machine delete' OR text='VM_STATE_GONE' OR text='Destroy VM complete' OR text='Delete virtual machine' OR text='Destroy VM called' OR text='VM_STATE_DELETING') ORDER BY ingest_timestamp ASC",
    "start": 1654576256000,
    "end": 1654583066000,
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

response2 = requests.get(f"https://api.mgmt.cloud.vmware.com/lint/query/0f87e6d8-9f5c-4a79-9dde-c964ec514f92", headers=headers)
print(response2.status_code)
print(json.dumps(response2.json(), indent=4))
logs_dict = response2.json()
#print(response2.json())
print("The no of logs extracted are:")

no_of_logs = len(logs_dict['processedResults'])
print(no_of_logs)
order = ["sddc_id", "epoch_timestamp", "timestamp", "hostname", "appname", "component",
         "severity", "operation_type", "message", "resource_name", "resource_type", "parent_resource_name", "parent_resource_type",
         "operation_id", "task_status"]

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
    details.append("VM Deletion")
    details.append(level_2['text'])
    key = 'vmw_vm_vmx_name'
    if key in level_2:
        details.append(level_2[key])
    else:
        details.append("")
    details.append("Virtual Machine")
    key = 'source_hostname'
    if key in level_2:
        details.append(level_2[key])
    else:
        details.append("")
    details.append("Host")
    key = 'vmw_opid'
    if key in level_2:
        details.append(level_2[key])
    else:
        details.append("")
    if 'Virtual machine delete completed' in level_2['text']:
        details.append("Success")
    else:
        details.append("In progress")
    structure = {key: value for key, value in zip(order, details)}
    store.append(structure)

client = MongoClient("mongodb://localhost:27017")
db = client['logs']
collection = db['Delete_VM_logs']
for entry in store:
    collection.insert_one(entry)