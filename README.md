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
2. 