# Security Spark
Playing around with commonly available security log formats and processing them in Spark.

 Picking up a subset of log samples from https://www.secrepo.com/

  Hello Security Spark world!
  
  This is simple code write up for processing security logs in Spark. Should be good enough for a student, learner 
  or a security practitioner.
  
  Picking up the log samples hosted at - https://www.secrepo.com/ , reading them via Spark and running some 
  basic checks to see if we find anything suspicious. 
  
  If you find juicy stuff, you can write it to an S3 bucket (security datalake), ELK stack (faster analysis) or some other temp datastore for correlation with
  other suspicious alerts. And you can create your own small SIEM.
  
  You can even bundle this jar and its dependencies to a docker images and run it over a K8 cluster and schedule it
  through Airflow or some other batch orchestration tool to create an ETL. 
  
  I intend to create this to have some working code for developing POCs, collection of useful spark functions for security data 
  processing use-cases. Will continue to add changes as and when I get time.
