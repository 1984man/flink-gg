package com.biubiu.stream.table.processing

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object  ProcessingTimeRestractSteam {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = if (planner == "blink") {  // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") {  // use flink planner in streaming mode
      StreamTableEnvironment.create(env)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }
   val ddlSource =
     """
       |create table user_behavior(
       |news_entry_id string,
       |country string,
       |impression bigint,
       |ts TIMESTAMP(3),
       |ts2 AS PROCTIME()
       |) with(
       |'connector.type'='kafka',
       |'connector.version'='0.10',
       |'connector.topic'='test-source',
       |'connector.startup-mode'='latest-offset',
       |'connector.properties.zookeeper.connect'='localhost:2181',
       |'connector.properties.bootstrap.servers'='localhost:9092',
       |'format.type'='json'
       |)
     """.stripMargin

    tEnv.sqlUpdate(ddlSource)

   // tEnv.sqlQuery("select news_entry_id,ts2 from user_behavior")..toAppendStream[Row].print()

    //val countSql  = "select news_entry_id ,count(impression) from user_behavior group by news_entry_id"
    val countSql =
      """
        |SELECT news_entry_id,hour(TUMBLE_START(ts2, INTERVAL '10' SECOND)), COUNT(*)
        |FROM dwd_table
        |GROUP BY news_entry_id,TUMBLE(ts2, INTERVAL '10' SECOND)
      """.stripMargin
    val table: Table = tEnv.sqlQuery(countSql)
    val result: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](table)

    result.print()

    env.execute()
  }
}
