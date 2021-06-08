## Airflow Setup

- Control Panel | Programs and Features | Turn Windows features on or off.
- Enable : Windows Subsystem for Linux
- Install ubuntu from microsoft play store and restart system
- Then in mobaxterm open windows wsl and run sudo apt-get install software-properties-common sudo apt-add-repository universe sudo apt-get update sudo apt-get install python-pip
- pip install apache-airflow
- airflow db init
- airflow webserver -p 8080
- airflow scheduler

## Neo4j Installation

- For neo4j I have used the following link https://www.liquidweb.com/kb/how-to-install-neo4j-on-ubuntu-20-04/
- I have installed the neo4j desktop

## Local Dev setup

- pip install -r requirements.txt
- Configure the .env details with the neo4j details
- run the pipeline from the airflow web ui
