from google.cloud import pubsub_v1
import glob
import os
import csv
import json
import time

# Retrieve and set API key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = glob.glob("*.json")[0]

csv_path = "./Labels.csv"
project_id = "sofe4620-100846922" # GCP project ID
topic_name = "pedestrian-occlusion-data"

pub_client = pubsub_v1.PublisherClient()
topic_path = pub_client.topic_path(project_id, topic_name)

with open(csv_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    headers = []

    for header in next(csv_reader):
        headers.append(header)

    for row in csv_reader:
        col_num = 0
        payload = {}
        for col in row:
            try:
                payload[headers[col_num]] = float(col)
            except ValueError:
                payload[headers[col_num]] = str(col)
            col_num += 1

        ftr = pub_client.publish(topic_path, json.dumps(payload).encode('utf-8'))
        ftr.result()
        time.sleep(0.001)
