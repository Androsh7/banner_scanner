import csv
import json

with open(file="banner_scanner/in.json", encoding="utf-8") as file:
    data = json.load(fp=file)

with open(file="banner_scanner/out.csv", mode="w", encoding="utf-8", newline="") as out_file:
    writer = csv.writer(out_file)
    writer.writerow(["ip", "port"])
    for subdict in data:
        writer.writerow([subdict["ip"], subdict["ports"][0]["port"]])
