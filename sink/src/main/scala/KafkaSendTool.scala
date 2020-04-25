/**
  * Created by yliu on 05/02/2018.
  */
import java.util.Properties

import com.google.gson.{Gson, JsonElement}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by yliu on 01/04/2017.
  */
object KafkaSendTool {
  def main(args: Array[String]): Unit = {
    val gson = new Gson()
    val path="/Users/yliu/workspace/flink-gg/sink/src/main/scala/record.txt"
    val source1  =scala.io.Source.fromFile(path).getLines

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("request.required.acks", "1")
    //props.put("producer.type", "async")
    val producer = new KafkaProducer[String,String](props)
    val TOPIC1="test-source"


    for (str <- source1) {
      val sessionEvents = gson.fromJson(str, classOf[JsonElement]).getAsJsonObject

      val message = new ProducerRecord(TOPIC1, sessionEvents.toString, sessionEvents.toString)
      for(i <- 0 to 100000) {
      producer.send(message)
      }
      //Thread.sleep(Random.nextInt(100))
    }




    producer.close()



    //val sessionEvents = gson.fromJson("", classOf[JsonElement]).getAsJsonObject
  }
}

