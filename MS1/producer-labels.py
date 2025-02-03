from google.cloud import pubsub_v1
import glob
import os
import csv

# Retrieve and set API key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = glob.glob("*.json")[0]

csv_path = "./Labels.csv"
project_id = "sofe4620-100846922" # GCP project ID
topic_names = [] # to be populated from the csv

pub_client = pubsub_v1.PublisherClient()

# open the csv in read
with open(csv_path, 'r') as csv_file:
    # open file reader
    csv_reader = csv.reader(csv_file)
    # read headers and make topics
    for header in next(csv_reader):
        topic_names.append(pub_client.topic_path(project_id, header))

    # read in the data and publish
    for row in csv_reader:
        topic_num = 0
        for col in row:
            msg = str(col).encode('utf-8')
            ftr = pub_client.publish(topic_names[topic_num], msg)
            ftr.result()
            topic_num += 1
        topic_num += 1