//package com.biubiu.sink.parquet
//
//import java.util.Properties
//import java.util.concurrent.TimeUnit
//
//import com.biubiu.sink.parquet.model.ResultPOJO
//import com.google.gson.{Gson, JsonElement}
//import org.apache.flink.api.common.serialization.SimpleStringEncoder
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//
//object TestStreamingParquet {
//  def main(args: Array[String]): Unit = {
//    val params = ParameterTool.fromArgs(args)
//    val planner = if (params.has("planner")) params.get("planner") else "flink"
//
//    // set up execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = if (planner == "blink") { // use blink planner in streaming mode
//      val settings = EnvironmentSettings.newInstance()
//        .useBlinkPlanner()
//        .inStreamingMode()
//        .build()
//      StreamTableEnvironment.create(env, settings)
//    } else if (planner == "flink") { // use flink planner in streaming mode
//      StreamTableEnvironment.create(env)
//    } else {
//      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
//        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
//        "example uses flink planner or blink planner.")
//      return
//    }
//
//    env.enableCheckpointing(60*1000L)
//
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
//    kafkaProps.setProperty("group.id", "t1")
//    val events = env.addSource(new FlinkKafkaConsumer011[String]("test-source", new SimpleStringSchema(), kafkaProps).setStartFromLatest())
//    events.print()
//
//    val events_pojo = events.map { line =>
//      val gson = new Gson()
//      val obj = gson.fromJson(line, classOf[JsonElement]).getAsJsonObject
//      ResultPOJO(obj.get("news_entry_id").getAsString, obj.get("ts").getAsLong)
//    }
//
//
//
//    val sink = StreamingFileSink
//      .forBulkFormat(new Path("/opt/tmp/"), CustomParquetAvroWriters.forReflectRecord[ResultPOJO]())
//      .withBucketCheckInterval(1 * 60 * 1000L)
//      .withBucketAssigner(CustomDateTimeBucketAssigner("yyyyMMdd/HH"))
//      .build()
//
//    events_pojo.addSink(sink)
//    events_pojo.print()
//
//    env.execute()
//  }
//}
