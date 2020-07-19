import os
import time
import logging
from io import BytesIO
import base64
from PIL import Image
import numpy as np
import cv2
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage


def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
    blob_name = msg.get_body().decode('utf-8')
    connection_string = os.environ["snibirkedastor_STORAGE"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    start_time = time.perf_counter()
    try:
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="grayscale", blob_name=blob_name)
        data = blob.download_blob().content_as_bytes(max_concurrency=1)
        dt = np.fromstring(data, dtype='uint8')
        gray = cv2.imdecode(dt, cv2.IMREAD_UNCHANGED)
        edges = cv2.Canny(gray, 60, 120)
        pil_image = Image.fromarray(edges)
        img_byte_arr = BytesIO()
        pil_image.save(img_byte_arr, format='JPEG')
        img_byte_arr = img_byte_arr.getvalue()
        #upload canny
        container_name = "canny"
        container_client = blob_service_client.get_container_client(container_name)
        container_client.upload_blob(name=blob.blob_name, data=img_byte_arr)
    except:
        pass
    end_time = time.perf_counter()
    logging.info(f"Container Name: {container_name}, Blob Name: {blob.blob_name}, Time Taken: {end_time - start_time}")
