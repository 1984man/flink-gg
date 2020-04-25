package com.biubiu.sink.clickhouse

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment



object ClickHouseSink {

  /**
    * 获取clickhousesink连接
    */
  val getClickHouseSink :(String,Array[TypeInformation[_]],Int)=>JDBCAppendTableSink = ( sql:String,types: Array[TypeInformation[_]],batchSize:Int)=>{

    val output =JDBCAppendTableSink.builder()
      .setDrivername("com.github.housepower.jdbc.ClickHouseDriver")
      .setDBUrl("jdbc:clickhouse://182.10.1.144:9001")
      .setUsername("default")
      //      .setPassword(JdbcConfig.PASS_WORD)
      .setParameterTypes(types:_*)
      .setBatchSize(batchSize)
      .setQuery(sql)
      .build()
    println("=====指标sink==="+sql)
    output
  }

  val insertToTable:(String,String,StreamTableEnvironment,TableSchema,Int)=>Unit =(tableName:String,sql:String,tableEnv:StreamTableEnvironment,schema:TableSchema,batchSize:Int)=>{
    println("=====数据库sink===")

    val output = getClickHouseSink(sql,schema.getFieldTypes,batchSize)
    println(schema.getFieldTypes+"====")

    tableEnv.registerTableSink(tableName,schema.getFieldNames,schema.getFieldTypes,output)
    println(output.getFieldNames+"===")

  }



}
