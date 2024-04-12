from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
import uuid
import os

class CustomXComBackendJSON(BaseXCom):
    # the prefix is optional and used to make it easier to recognize
    # which reference strings in the Airflow metadata database
    # refer to an XCom that has been stored in a GCS bucket
    PREFIX = "xcom_gcs://"
    BUCKET_NAME = "gcs-xcom-backend-example"

    @staticmethod
    def serialize_value(
        value,
        key=None,
        task_id=None,
        dag_id=None,
        run_id=None,
        map_index= None,
        **kwargs
    ):
        
        # the connection to GCS is created by using the GCShook with 
        # the conn id configured in Step 3
        hook = GCSHook(gcp_conn_id="gcs_xcom_backend_conn")
        # make sure the file_id is unique, either by using combinations of
        # the task_id, run_id and map_index parameters or by using a uuid
        filename = "data_" + str(uuid.uuid4()) + ".json"
        # define the full GCS key where the file should be stored
        gs_key = f"{run_id}/{task_id}/{filename}"

        # write the value to a local temporary JSON file
        with open(filename, 'a+') as f:
            json.dump(value, f)

        # load the local JSON file into the GCS bucket
        hook.upload(
            filename=filename,
            object_name=gs_key,
            bucket_name=CustomXComBackendJSON.BUCKET_NAME,
        )

        # remove the local temporary JSON file
        os.remove(filename)

        # define the string that will be saved to the Airflow metadata 
        # database to refer to this XCom
        reference_string = CustomXComBackendJSON.PREFIX + gs_key

        # use JSON serialization to write the reference string to the
        # Airflow metadata database (like a regular XCom)
        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result):
        # retrieve the relevant reference string from the metadata database
        reference_string = BaseXCom.deserialize_value(result=result)
        
        # create the GCS connection using the GCSHook and recreate the key
        hook = GCSHook(gcp_conn_id="gcs_xcom_backend_conn")
        gs_key = reference_string.replace(CustomXComBackendJSON.PREFIX, "")

        # download the JSON file found at the location described by the 
        # reference string to a temporary local folder
        filename = hook.download(
            object_name=gs_key,
            bucket_name=CustomXComBackendJSON.BUCKET_NAME,
            filename="my_xcom.json"
        )

        # load the content of the local JSON file and return it to be used by
        # the operator
        with open(filename, 'r') as f:
            output = json.load(f)

        # remove the local temporary JSON file
        os.remove(filename)

        return output