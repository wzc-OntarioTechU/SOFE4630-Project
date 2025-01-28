from google.cloud import pubsub_v1
import glob
import json
import os

# Retrieve and set API key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = glob.glob("*.json")[0]

project_id = "sofe4620-100846922" # GCP project ID
topic_names = ["Timestamp", "Car1_Location_X", "Car1_Location_Y", "Car1_Location_Z", "Car2_Location_X", "Car2_Location_Y", "Car2_Location_Z", "Occluded_Image_view", "Occluding_Car_view", "Ground_Truth_View", "pedestrianLocationX_TopLeft", "pedestrianLocationY_TopLeft", "pedestrianLocationX_BottomRight", "pedestrianLocationY_BottomRight"] # expected topics
sub_path = []
topic_path = []

# create client
sub_client = pubsub_v1.SubscriberClient()

# build topic paths and sub paths
for topic in topic_names:
    sub_path.append(sub_client.subscription_path(project_id, '{}-sub'.format(topic)))
    topic_path.append('projects/{}/topics/{}'.format(project_id, topic))

# modified from provided
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    message_data = str(message.data)
    print("Consumed record with value : {}" .format(message_data))
    message.ack()

# subscribe to each topic
with sub_client:
    futures = []
    for path in sub_path:
        futures.append(sub_client.subscribe(path, callback))
    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        for future in futures:
            future.cancel()