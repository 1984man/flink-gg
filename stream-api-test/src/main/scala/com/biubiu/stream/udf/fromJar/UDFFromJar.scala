package com.biubiu.stream.udf.fromJar

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row




object UDFFromJar {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


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

    /*
    aaa:3> (true,e1,1587796685000,2020-04-25T14:38:05)
    aaa:3> (true,e1,1587796685000,2020-04-25T14:38:05)
    aaa:3> (true,e1,1587796686000,2020-04-25T14:38:06)
    aaa:3> (true,e1,1587796686000,2020-04-25T14:38:06)
    aaa:3> (true,e2,1587801554000,2020-04-25T15:59:14)
    aaa:3> (true,e2,1587801554000,2020-04-25T15:59:14)
     */

    //只需要注册就可以
    tEnv.sqlUpdate(
      """
        |create FUNCTION f1 as 'com.biubiu.udf.T'
      """.stripMargin)

    val ddlSource =
      """
        |create table user_behavior(
        |news_entry_id string,
        |ts bigint,
        |ts1 AS TO_TIMESTAMP(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),
        |WATERMARK FOR ts1 AS ts1 - INTERVAL '5' SECOND
        |) with(
        |'connector.type'='kafka',
        |'connector.version'='universal',
        |'connector.topic'='test-source',
        |'connector.startup-mode'='latest-offset',
        |'connector.properties.zookeeper.connect'='localhost:2181',
        |'connector.properties.bootstrap.servers'='localhost:9092',
        |'update-mode' = 'append',
        |'format.type'='json'
        |)
      """.stripMargin

    tEnv.sqlUpdate(ddlSource)


    //2020-04-08T19:46:44
    //    val countSql =
    //      """select news_entry_id,ts,ts1 from user_behavior"""

    //val countSql  = "select news_entry_id ,count(impression) from user_behavior group by news_entry_id"

    tEnv.toAppendStream[Row](tEnv.sqlQuery("""select news_entry_id,f1( cast (123 as bigint)),ts,ts1 from user_behavior""")).print()

        val sink: StreamingFileSink[String] = StreamingFileSink
          .forRowFormat(new Path("/opt/tmp/xx1/"), new SimpleStringEncoder[String]("UTF-8")) // 所有数据都写到同一个路径
          .build()


    tEnv.toAppendStream[Row](tEnv.sqlQuery("""select news_entry_id,f1( cast (123 as bigint)),ts,ts1 from user_behavior"""))
      .map(line=>(line.toString))
      .addSink(sink)


    env.execute()
  }
}
