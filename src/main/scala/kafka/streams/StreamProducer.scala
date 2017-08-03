package kafka.streams

import java.time.Instant
import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.math.random

object StreamProducer {

  val startTime: Long = Instant.now().toEpochMilli

  val TO_TOPIC: String = "stream-events"

  def getTimestamp(x: Int): Long = startTime + (x * 20)

  def getValue(x: Int): String = {
    x.toString
  }

  def getRecord(x: Int, partitions: Int): (ProducerRecord[String, Array[Byte]], InputMessage) = {
    val ts           = getTimestamp(x)
    val k            = UUID.randomUUID().toString
    val partition    = x % partitions
    val inputMessage = InputMessage(k, getValue(x), s"P:$partition", ts)
    val v            = AvroThings.getBAOS(Seq(inputMessage))
    val record       = new ProducerRecord(TO_TOPIC, partition, ts, k, v)
    (record, inputMessage)
  }

  def produce(partitions: Int = 2, messagesPerPartitionPerBatch: Int = 6, sleepTimeBase: Int = 4000): Unit = {

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)

    (0 to 10000).toStream.foreach { r =>
      val messageNumber = messagesPerPartitionPerBatch * partitions
      Range(r * messageNumber, (r + 1) * messageNumber)
        .partition(_ % partitions == 0) match {
        case (a, b) =>
          val records = a.map(x => getRecord(x, partitions)) ++ b.map(x => getRecord(x, partitions))
          records.foreach(x => producer.send(x._1))
          val inputMessages: Seq[InputMessage] = records.map(_._2)
          val baos                             = AvroThings.getBAOS(inputMessages)
          AvroThings.writeAvro[InputMessage](baos, "input")
      }
      val sleepTime: Int = (random * sleepTimeBase).toInt
      println(s"Producer sleeping for $sleepTime seconds")
      Thread.sleep(sleepTime)
    }
  }
}
