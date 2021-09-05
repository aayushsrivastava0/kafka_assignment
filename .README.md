# Assignment Given

### Kafka Pub-Sub Project using sbt
##### In this project producer publish a user message into a Kafka topic and consume it using the Kafka Producer and Consumer APIs.
##### Once the message is consumed from a consumer, it is written  into a file named student.txt.
##### Message Structure
##### User message --->{"id":"1","name":"some_name","age":"24","course":"BTech."}



#  Library dependency for project
#### 1. kafka-clients
#### 2. slf4j-simple
#### 3. circe-core
#### 4. circe-generic
#### 5. circe-parser



# Installation of Sbt and scala
#### 1.You must first install a JDK. We recommend AdoptOpenJDK JDK 8 or JDK 11.

#### 2.Next, install Sbt by typing the following commands:

```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```

# Usage

#### 1. Clone the project.
```bash
git clone (repo link)
```

#### 2.Download the latest Kafka release and extract it:
```bash
$ tar -xzf kafka_2.13-2.8.0.tgz
$ cd kafka_2.13-2.8.0
```


#### 3.Open the folder where Apache Kafka is installed and Start Apache Zookeeper
```bash
$ bin/zookeeper-server-start.sh ../config/zookeeper.properties

```
#### 4. Start the Kafka server
```bash
 $ bin/kafka-server-start.sh ../config/server.properties
```


#### 4. First run the KafkaScalaConsumer class and then KafkaScalaProducer class for getting the output

#### 5. A information.txt will be created in target folder that will display consumed details,after running.
