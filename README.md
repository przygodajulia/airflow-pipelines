# AIRFLOW Capstone Project </br>

## Configuration needed before starting the project
To make sure that everything works as expected please make the following changes:
1. **docker-compose.yaml** file please change the first part of the following volume: "/Users/jprzygoda/my_files:/data_folder/"
to location on your local system. This the location where sensor will poke for file.

2. In the Airflow UI please add following connections:
- Connection_Id: 'my_filesystem', Connection_Type: File, Extra: {"path": "/data_folder/"}
- Connection_Id: 'postgres_localhost', Connection_Type: Postgres, Login: airflow, Port: 5432 

3. Please note that this code uses slack and pandas modules

## Files description
Zip_dag_contents (in zip_dag.zip)
- **trigger_dag.py** - contains file sensor that checks for a file at specified location, triggers dag_1 from jobs_dag.py
processes result in SubDag and then sends message to Slack channel
- **smart_file_sensor.py** - for smart file sensor used in trigger_dag.py

Dags main folder:
- **jobs_dag.py** - dynamically generated dags, dag_1 will be triggered by dag_trigger
- **subdag.py** - contains a factory function for SubDag from trigger_dag.py
- **slack_call.py** - does not contain any dags, this is just a script that is being run by BashOperator. Due to some 
import problems I have placed them in the same folder. Please change the channel id in this file if you would like to
check messages for your own channel.
- **custom_operator.py** and **smart_file_sensor.py** - contain some custom DAG definitions
- **functional_dag_exercise.py** - not part of the main project, this was included as an exercise for functional DAGs

## Running the project
1. Start Airflow and add following connections in the UI.
2. Start the project with command: </br>
_docker-compose --profile flower up_ if you would like to access flower and see how
work is distributed among different workes </br>
_docker-compose up_ if you do not need to access flower.
3. One the vault container is active execute following commands (this is needed when first starting Airflow): </br>
_docker exec -it VAULT_DOCKER_ID sh_ </br>
_vault login ZyrP7NtNw0hbLUqu7N3IlTdO_ </br>
_vault secrets enable -path=airflow -version=2 kv_ </br>
_vault kv put airflow/variables/slack_token value=xoxb-4625223963587-4618765316326-NPBleFGauzjCfYFk6TFgQgH9_ 
to use my test slack token </br>
4. In your location put data.csv file that can be sensed by SmartFileSensor
5. Unpause dag_trigger and dag_1.
6. Trigger dag_trigger it should automatically start dag_1, imitate the process of processing the data.csv file,
create and insert relevant rows to database tables, remove input data file and create finished file with timestamp.