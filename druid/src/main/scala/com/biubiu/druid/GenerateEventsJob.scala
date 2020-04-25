package com.biubiu.druid

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.biubiu.druid.model.SimpleEvent
import com.metamx.tranquility.flink.BeamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
//
object GenerateEventsJob {


  def kafkaProperties(bootstrapServer: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServer)
    properties
  }

  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "my-druid-reader")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    val stream:DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("test-source", new SimpleStringSchema(), kafkaProps))

//    val sink = new KafkaSink("s")
//    sink.writeToKafka(stream)
    val eventStream :DataStream[SimpleEvent] = stream.map { line =>
      val json = JSON.parseObject(line)
      //{"news_entry_id":"e1","country":"ng","language":"en","impression":10,"click":100}
      val news_entry_id = json.getString("news_entry_id")
      val country = json.getString("country")
      val language = json.getString("language")
      val impression = json.getLong("impression")
      val click = json.getLong("click")

      SimpleEvent(System.currentTimeMillis(),news_entry_id, country, language, impression, click)
    }

    val zkConnect = "localhost:2181"
    val sink = new BeamSink[SimpleEvent](new SimpleEventBeamFactory(zkConnect))

    println("------------------------------------------")

    eventStream.addSink(sink)

    env.execute("Generate events")
  }



}
