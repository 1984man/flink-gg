package com.biubiu.sink.mysql.ddl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._

object TestStreamingMySQLByDDL {
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


    val ddlSource =
      """
        |create table user_behavior(
        |news_entry_id string,
        |ts bigint,
        |ts1 AS TO_TIMESTAMP(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),
        |WATERMARK FOR ts1 AS ts1 - INTERVAL '5' SECOND
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


    val ddlSink =
      """
        |CREATE TABLE t_table (
        |  id string,
        |  ts bigint
        |) WITH (
        |  'connector.type' = 'jdbc', -- required: specify this table type is jdbc
        |
        |  'connector.url' = 'jdbc:mysql://localhost:3306/dianping', -- required: JDBC DB url
        |
        |  'connector.table' = 'test',  -- required: jdbc table name
        |
        |  'connector.driver' = 'com.mysql.jdbc.Driver', -- optional: the class name of the JDBC driver to use to connect to this URL.
        |                                                -- If not set, it will automatically be derived from the URL.
        |
        |  'connector.username' = 'root', -- optional: jdbc user name and password
        |  'connector.password' = 'root',
        |  'connector.lookup.max-retries' = '3', -- optional, max retry times if lookup database failed
        |
        |  -- sink options, optional, used when writing into table
        |  'connector.write.flush.max-rows' = '5000', -- optional, flush max size (includes all append, upsert and delete records),
        |                                             -- over this number of records, will flush data. The default value is "5000".
        |  'connector.write.flush.interval' = '2s', -- optional, flush interval mills, over this time, asynchronous threads will flush data.
        |                                           -- The default value is "0s", which means no asynchronous flush thread will be scheduled.
        |  'connector.write.max-retries' = '3' -- optional, max retry times if writing records to database failed
        |)
        |
      """.stripMargin

    tEnv.sqlUpdate(ddlSink)

    //2020-04-08T19:46:44
    //    val countSql =
    //      """select news_entry_id,ts,ts1 from user_behavior"""

    //val countSql  = "select news_entry_id ,count(impression) from user_behavior group by news_entry_id"

  tEnv.toAppendStream[Row](tEnv.sqlQuery("select news_entry_id,ts from user_behavior")).print()

    val dmlSql =
      """
        |insert into t_table
        |select news_entry_id as id,ts from user_behavior
      |
      """.stripMargin
    tEnv.sqlUpdate(dmlSql)




    env.execute()
  }
}
