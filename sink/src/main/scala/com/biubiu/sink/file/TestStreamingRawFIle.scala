//package com.biubiu.sink.file
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//import java.util.concurrent.TimeUnit
//
//import com.biubiu.sink.parquet.CustomDateTimeBucketAssigner
//import org.apache.flink.api.common.serialization.SimpleStringEncoder
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.streaming.api.scala._
//
//object TestStreamingRawFIle {
//  def main(args: Array[String]): Unit = {
//    val params = ParameterTool.fromArgs(args)
//    val planner = if (params.has("planner")) params.get("planner") else "blink"
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
//    env.enableCheckpointing(5000L)
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
//    kafkaProps.setProperty("group.id", "t1")
//    val events = env.addSource(new FlinkKafkaConsumer011[String]("test-source", new SimpleStringSchema(), kafkaProps))
//    events.print()
//
//    val po: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
//      .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
//      .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
//     // .withMaxPartSize(1024 * 1024 * 1024)
//      .withMaxPartSize(1024*1024)
//      .build[String, String]()
//
//
//    val sink: StreamingFileSink[String] = StreamingFileSink
//      .forRowFormat(new Path("/opt/tmp/xx/"), new SimpleStringEncoder[String]("UTF-8")) // 所有数据都写到同一个路径
//      .withBucketCheckInterval(1000L)
//      .withBucketAssigner(new CustomBucketAssigner())
//      .withRollingPolicy(po)
//      .build()
//    events.addSink(sink)
//
//    env.execute()
//  }
//
//  val df = new SimpleDateFormat("yyyyMMdd/HH");
//
//}
//
//
//
