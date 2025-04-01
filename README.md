<p>The project simulates IoT real-time data processing using Apache technologies. The app run on Linux</p>
<h4>System requirements</h4>
<ul>
  <li>Opetration system: Linux / Ubuntu</li>
  <li>Software: Apache Spark, Kafka, Zookeeper</li>
  <li>Packages and library: Python, Java, Pyspark, Confluent Kafka, Confluent Spark</li>
</ul>
<h4>How to set up the requirements?#</h4>
<ol>
  <li>start a zookeeper session that will manage the Kafa topics</li>
  <li>start a Kafka serve that will handle the data and messaging</li>
  <li>create the Kafka topics that subscribe the data stream. Two topics (sensor1 and sensor2) are created to demonstrate Kafka simultaneous data streaming capabilities</li>
  <li>consume the Kafka topics</li>
  <li>run the python scripts (simulate_topic1_sensors.py / simulate_topic1_sensors2.py to simulate IoT data streaming. The data shall simsimultaneously capture by the Kafka consumer and display on the shell</li>
</ol>
<hr>
<h4>Real-time data processing</h4>
<p>Apache technologies are excellent tools for real-time data analysis and processing.2 scripts (kafka_multistream.py spark_multistreams.py*) and are written to demostrate the function<p>
<p>These scripts should be run upon the above data streaming is running. It handles data generated from multiple sources, filter data and export them into CSV files</p>

<p>#there are many online resources on how to provision the Apache environment</p>
<p>*sample code to initiate the PySpark script ~/Apache/spark/spark-3.5.3-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_multistreams.py</p>
