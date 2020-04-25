package com.biubiu.sink.mysql.schema

import com.biubiu.sink.mysql.schema.MysqlSinkWithSchema.getMysqlSink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableSchema}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row


object EventTimeRestractSteamWithLongTSWithUDF {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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


    val ddlSource =
      """
        |create table user_behavior(
        |news_entry_id string,
        |ts bigint
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


    //2020-04-08T19:46:44
    val countSql =
      """select news_entry_id,ts from user_behavior"""
    //this is the source
    val source = tEnv.sqlQuery(countSql)
    val tableSchema = source.getSchema

    tableSchema.getFieldDataTypes.foreach(x => println(x.getLogicalType))
    println("====================")
    tableSchema.getFieldNames.foreach(x => println(x))


    // tEnv.toAppendStream[Row](table).print()

    val sql =
      """
      insert into test (id, ts) values(?, ?)
      """.stripMargin

    val output = getMysqlSink(sql, tableSchema.getFieldTypes)

    tEnv.registerTableSink("xx_table", tableSchema.getFieldNames, tableSchema.getFieldTypes, output)

    source.insertInto("xx_table")

    env.execute()
  }
}
