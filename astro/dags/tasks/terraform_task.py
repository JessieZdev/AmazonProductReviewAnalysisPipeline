import json
from airflow.models import Variable
import os 

def store_terraform_outputs_as_variables():
    terraform_outputs_path = os.path.join(os.environ['AIRFLOW_HOME'], 'include/terra_confi/info.json')

    # Load the JSON file
    with open(terraform_outputs_path, 'r') as file:
        terraform_outputs = json.load(file)

    # Iterate through the items in the JSON dictionary
    for key, value in terraform_outputs.items():
        variable_value = value['value'] if isinstance(value, dict) else value
        Variable.set(key, variable_value)
        
        
