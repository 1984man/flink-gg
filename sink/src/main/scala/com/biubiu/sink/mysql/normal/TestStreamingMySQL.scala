package com.biubiu.sink.mysql.normal

import java.util.Properties

import com.biubiu.sink.parquet.model.ResultPOJO
import com.google.gson.{Gson, JsonElement}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._


object TestStreamingMySQL {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = if (planner == "blink") { // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") { // use flink planner in streaming mode
      StreamTableEnvironment.create(env)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }

    env.enableCheckpointing(60*1000L)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "t1")
    val events = env.addSource(new FlinkKafkaConsumer011[String]("test-source", new SimpleStringSchema(), kafkaProps).setStartFromLatest())
    events.print()

    val events_pojo = events.map { line =>
      val gson = new Gson()
      val obj = gson.fromJson(line, classOf[JsonElement]).getAsJsonObject
      ResultPOJO(obj.get("news_entry_id").getAsString, obj.get("ts").getAsLong)
    }


    events_pojo.addSink(new SinkToMySQL())
    events_pojo.print()

    env.execute()
  }
}
