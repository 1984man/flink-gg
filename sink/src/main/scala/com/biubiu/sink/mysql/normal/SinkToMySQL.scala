package com.biubiu.sink.mysql.normal

import java.sql.{Connection, PreparedStatement}

import com.biubiu.sink.parquet.model.ResultPOJO
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class SinkToMySQL extends RichSinkFunction[ResultPOJO]{

  var ps :PreparedStatement = _
  var dataSource :BasicDataSource = _
  var connection :Connection = _


  def getConnection(dataSource: BasicDataSource): Connection = {
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUrl("jdbc:mysql://localhost:3306/dianping")
    dataSource.setUsername("root")
    dataSource.setPassword("root")

    dataSource.setInitialSize(10)
    dataSource.setMaxTotal(10)
    dataSource.setMinIdle(2)

    var conn = dataSource.getConnection()
    conn
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataSource = new BasicDataSource()
    connection = getConnection(dataSource)
    val sql = "insert into test(id,ts) values(?,?)"
    ps = connection.prepareStatement(sql)

  }

  override def close(): Unit ={
    super.close()
    ps.close()
    connection.close()
  }

  override def invoke(value: ResultPOJO, context: SinkFunction.Context[_]): Unit = {
          ps.setString(1,value.id)
    println(value.ts)
          ps.setLong(2,value.ts)
    ps.executeUpdate()
  }
}
