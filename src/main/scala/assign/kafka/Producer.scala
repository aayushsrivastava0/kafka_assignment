package assign.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Scanner
import io.circe.syntax.EncoderOps
import io.circe.generic.auto._
object KafkaScalaProducer extends App {

  val scanner = new Scanner(System.in)

  val props = new Properties()


  props.put("bootstrap.servers", "localhost:9092")

  props.put("client.id", "ScalaProducerExample")

  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")

  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")

  props.put("acks", "1")


  props.put("retries", "0")

  props.put("batch.size", "16384")

  props.put("linger.ms", "1")

  props.put("buffer.memory", "33554432")


  val producer: KafkaProducer[String, String] =
    new KafkaProducer[String, String](props)


  val topic = "kafka-topic-kip"

  println(s"Sending Records in Kafka Topic [$topic]")

  for (i <- 1 to 10) {

    val record: ProducerRecord[String, String] =
      new ProducerRecord(topic,
        i.toString,
        userDetails(i).asJson.toString)
    println(record)

    producer.send(record)
  }


  producer.close()


  def userDetails(id: Int): Student = {
    var name = ""
    var course = ""
    var age = 0
    println("Enter the name")
    name = scanner.nextLine()

    println("Enter the course")
    course = scanner.nextLine()

    println("Enter age")
    age = scanner.nextInt()
    scanner.nextLine()

    Student(id, name, course, age)

  }
}

case class Student(id: Int, name: String, course: String, age: Int)
