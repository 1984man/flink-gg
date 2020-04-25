package com.biubiu.sink.mysql.schema

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._

object MysqlSinkWithSchema {
  /**
    * 获取mysqlsink连接
    */
  val getMysqlSink: (String, Array[TypeInformation[_]]) => JDBCAppendTableSink =
    (sql: String, types: Array[TypeInformation[_]]) => {

      val output = JDBCAppendTableSink.builder()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl("jdbc:mysql://localhost:3306/dianping")
        .setUsername("root")
        .setPassword("root")
         .setParameterTypes(types:_*)
        //  .setBatchSize(batchSize)
        .setBatchSize(1)
        .setQuery(sql)
        .build()
      println("=====指标sink===" + sql)
      output
    }


  val insertToTable:(String,String,StreamTableEnvironment,TableSchema)=>Unit
  =(tableName:String,sql:String,tableEnv:StreamTableEnvironment,schema:TableSchema)=>{
    println("=====数据库sink===")
    val output = getMysqlSink(sql,schema.getFieldTypes)
  //  println(schema.getFieldTypes+"====")
    tableEnv.registerTableSink(tableName,schema.getFieldNames,schema.getFieldTypes,output)
  //  println(output.getFieldNames+"===")

  }
}
