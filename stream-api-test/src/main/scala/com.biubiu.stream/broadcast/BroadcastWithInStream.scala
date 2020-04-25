package com.biubiu.stream.broadcast


import java.util.Properties

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector


object  BroadcastWithInStream {
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

    //make a broadcast
    val mapDesc = new MapStateDescriptor(
      "rule",
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO);
    val value: DataStream[(String, String)] = env.fromCollection(Seq(("a1", "a1_value"), ("a2", "a2_value")))

    val bdata  = value.broadcast(mapDesc)

//


    val name = "test-source"
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "t1")
    val source1 = env.addSource(new FlinkKafkaConsumer011[String](name, new SimpleStringSchema(), kafkaProps))


    val name2 = name+"-side"
    val source2 = env.addSource(new FlinkKafkaConsumer011[String](name2, new SimpleStringSchema(), kafkaProps))


    val source = source1.union(source2)

    val broadcastConnected: BroadcastConnectedStream[String, (String, String)] = source.connect(bdata)


    val outputTag = OutputTag[String]("side-output")
    val result = broadcastConnected.process(new BroadcastProcessFunction[String,(String,String),String] {
      override def processElement(value: String, ctx: BroadcastProcessFunction[String, (String, String), String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val id = "a"
        val id_transfer: String = ctx.getBroadcastState(mapDesc).get(id)
        if(id_transfer !=null){
          out.collect(id_transfer)
        }else{
          println("value in side"+value)
          ctx.output(outputTag, value)
        }

      }

      override def processBroadcastElement(value: (String, String), ctx: BroadcastProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
        ctx.getBroadcastState(mapDesc).put(value._1,value._2)
      }
    })
    result.print()

    //如果没有join上 就直接写到另外一个kafka 用来从新处理
    val sideOutPut = result.getSideOutput(outputTag)

    //val sink = new KafkaSink(name2)
   // sink.writeToKafka(sideOutPut)




    env.execute()
  }
}

