import os
from google.cloud import pubsub_v1

#this is to allow a authenticated user on the project to work on bigquerry
Key_path = "C:\\Users\Brad\Documents\model-craft-342921-ea36cdb339e7.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Key_path


publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/model-craft-342921/topics/web_intake'

data = 'This is a test for pubsub!'
data = data.encode('utf-8')

future = publisher.publish(topic_path, data)
print(f'published message id {future.result()}')