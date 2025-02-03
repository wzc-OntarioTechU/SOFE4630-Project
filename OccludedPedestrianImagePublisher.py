from google.cloud import pubsub_v1
import glob
import base64
import os
import time

# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]
project_id="SOFE4620-100846922"
topic_name = "pedestrian-occlusion-image"
image_dir = "./Dataset_Occluded_Pedestrian"

pub_client = pubsub_v1.PublisherClient(publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
topic_path = pub_client.topic_path(project_id, topic_name)

for file_name in os.listdir(image_dir):
    if file_name.endswith(".png"):
        with open(os.path.join(image_dir, file_name), 'rb') as image_file:
            img_data = base64.b64decode(image_file.read())
            try:
                ftr = pub_client.publish(topic_path, img_data, ordering_key=file_name)
                ftr.result()
                print("Published image:", file_name)
            except:
                print("Failed to publish image:", file_name)
            time.sleep(0.001) # so that the application integration rate limit is not exceeded
            