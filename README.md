Command to build jar 
sbt package

Move the jar to the Docker Spark 
docker cp /Users/pramesh/Desktop/spark-examples/target/scala-2.11/spark-examples_2.11-0.1.jar aafd6c6f0b75:/jars

Move the data 
docker cp /Users/pramesh/Desktop/spark-examples/data/indian-premier-league-csv-dataset  aafd6c6f0b75:/root/data/indian-premier-league-csv-dataset 

Spark Submit
./bin/spark-submit   --class org.spark.examples.sql.SQLMain  /jar/spark-examples_2.11-0.1.jar