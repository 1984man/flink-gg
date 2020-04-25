package com.biubiu.stream.table.eventtime

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object  EventTimeRestractSteam {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    //2020-04-08T19:46:44.907
    //TIMESTAMP(3)类型


   val ddlSource =
     """
       |create table user_behavior(
       |news_entry_id string,
       |ts TIMESTAMP(3),
       |WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
       |) with(
       |'connector.type'='kafka',
       |'connector.version'='0.11',
       |'connector.topic'='test-source',
       |'connector.startup-mode'='latest-offset',
       |'connector.properties.zookeeper.connect'='localhost:2181',
       |'connector.properties.bootstrap.servers'='localhost:9092',
       |'update-mode' = 'append',
       |'format.type'='json'
       |)
     """.stripMargin

    tEnv.sqlUpdate(ddlSource)

    //val countSql ="select news_entry_id,ts from user_behavior"

    //val countSql  = "select news_entry_id ,count(impression) from user_behavior group by news_entry_id"



    val countSql =
      """
        |SELECT news_entry_id,hour(tumble_start(ts, interval '10' second)),count(*)
        |FROM user_behavior
        |GROUP BY news_entry_id,TUMBLE(ts, INTERVAL '10' SECOND)
      """.stripMargin
    val table: Table = tEnv.sqlQuery(countSql)
    //table.printSchema()
    tEnv.toRetractStream[Row](table).print("aaa")


    env.execute()
  }
}
