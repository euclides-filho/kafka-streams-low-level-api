package kafka.streams

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor.{ProcessorSupplier, StateStore, StateStoreSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.Stores.PersistentKeyValueFactory

class StreamConsumer {

  val name               = "app-stream"
  val FROM_TOPIC: String = StreamProducer.TO_TOPIC
  val TO_TOPIC: String   = "stream-events-agg"

  val streamProcessor: ProcessorSupplier[String, Array[Byte]] = () => new StreamProcessor()
  val streamWriter: ProcessorSupplier[String, Array[Byte]]    = () => new StreamWriter()

  private val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, name)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass)
    p
  }

  private val store: PersistentKeyValueFactory[String, Array[Byte]] =
    Stores
      .create("STORE")
      .withStringKeys()
      .withByteArrayValues()
      .persistent()

  private val builtStore: StateStoreSupplier[_ <: StateStore] = store.build()

  private val builder: TopologyBuilder = new TopologyBuilder()
    .addSource("SOURCE", FROM_TOPIC)
    .addProcessor("PROCESSOR", streamProcessor, "SOURCE")
    .addProcessor("WRITER", streamWriter, "PROCESSOR") // process the result of the streamProcessor
    .addSink("SINK", TO_TOPIC, Serdes.String().serializer(), Serdes.ByteArray().serializer(), "PROCESSOR") // publishes to a new topic
    .addStateStore(builtStore, "PROCESSOR")
    .connectProcessorAndStateStores("PROCESSOR", "STORE")

  private val streams: KafkaStreams = new KafkaStreams(builder, config)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))

  println("StreamConsumer Started")

}
