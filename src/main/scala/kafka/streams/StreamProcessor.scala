package kafka.streams

import java.time.Instant
import java.util.UUID

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

import scala.math.random
import scala.collection.JavaConverters._

class StreamProcessor extends Processor[String, Array[Byte]] {

  private var processor: ProcessorContext = _
  private var state: KVStoreType          = _

  private var lastFlush: Long = Instant.now().toEpochMilli

  val MAX_FLUSH_LAG: Long     = 5  // seconds
  val MAX_FLUSH_MESSAGES: Int = 20 // messages

  private def setLastFlush(): Unit = {
    lastFlush.synchronized {
      lastFlush = Instant.now().toEpochMilli
    }
  }

  private def flushLagSeconds(): Long = {
    (Instant.now().toEpochMilli - lastFlush) / 1000
  }

  override def init(context: ProcessorContext): Unit = {
    processor = context
    context.schedule(100)
    state = context.getStateStore("STORE").asInstanceOf[KVStoreType]
  }

  override def process(key: String, value: Array[Byte]): Unit = {
    state.put(key, value)
  }

  private def flush(): Unit = {
    val allState = state.all().asScala.toSeq
    if (allState.nonEmpty) {
      val batchId = UUID.randomUUID().toString
      println(s"allState ${allState.size}")
      val messages: Seq[OutputMessage] =
        allState.flatMap(x =>
          AvroThings.getResultFromBA[InputMessage](x.value).map(value => OutputMessage(batchId, value)))
      val baos = AvroThings.getBAOS(messages)

      processor.forward(batchId, baos)
      setLastFlush()
      processor.commit()
      allState.foreach(x => state.delete(x.key))
    }
  }

  override def punctuate(timestamp: Long): Unit = {
    println(s"punctuate ${state.approximateNumEntries()} ${flushLagSeconds()}")
    if (state.approximateNumEntries() > MAX_FLUSH_MESSAGES) {
      println("Flushing for SIZE")
      flush()
    } else if (flushLagSeconds() > MAX_FLUSH_LAG) {
      println("Flushing for TIME")
      flush()
    }
  }

  override def close(): Unit = {}
}
