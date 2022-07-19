# Online Taxi Service
Real-time system for instant analysis, illustration, forecasting, and storage on online taxi service's data. for adding 
these functionalities, some tools are used in this project:
* spark for machine learning
* kafka for stream management
* elasticsearch and kibana for plot primary diagrams and data management
* redis for realtime app for user queries
* cassandra for data management
## Dataset
you can access the dataset files in this [link](https://drive.google.com/drive/folders/1rt3LWG1KMenBejpJ86WWYrlohBaFNQCl).

## Prerequisites for running
1. run kafka server<br/>
make sure "delete.topic.enable=true" is placed in the server.properties and enter bootstrap-server address in .env file
2. run Spark cluster<br/>
3. run Elasticsearch server
4. run Kibana server
5. run Cassandra server
6. run redis server
7. install all packages. <br/>
pip install -r requirements.txt
8. run main.py with blew comment:<br/>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,graphframes:graphfr
ames:0.8.2-spark3.1-s_2.12 --master spark://<master_ip>:<port> main.py<br/>
<b>this project run on spark 3.1.3 and the version of packages is according to spark-version. 
make sure version of packages is consistent with spark version on your system</b>


## Kibana
in this project some visualizations and one dashboard is designed. visualizations can be used as one of the visualization 
in your own dashboard or you can use whole dashboard. in "Saved object" you can import objects.