package com.biubiu.druid

import com.biubiu.druid.model.SimpleEvent
import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid._
import com.metamx.tranquility.flink.BeamFactory
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularities
import io.druid.query.aggregation.{CountAggregatorFactory, LongSumAggregatorFactory}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.joda.time.DateTime
import org.scala_tools.time.Imports.Period

class SimpleEventBeamFactory(zkurl:String) extends BeamFactory[SimpleEvent]
{

  lazy val makeBeam: Beam[SimpleEvent] = {

    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      zkurl,//"localhost:2181",zookeeper地址
      //重试之间的时间间隔，重试的最大时间间隔，重试次数
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    //配置参数：http://druid.io/docs/latest/configuration/index.html

    //下面配置的参数用于生成tranquality.json文件，故可以和json文件中的元素字段对应配置
    //indexService   区分节点类型：初步了解接单类型有 druid/overlord  ,druid/coordinator,druid/broker ,druid/middleManager,druid/historical
    //****对应json:druid.selectors.indexing.serviceName
    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.==The druid.service name of the indexing service Overlord node. To start the Overlord with a different name, set it with this property.
    //discovery znode   druid在zk上存储的路径
    //****对应json:druid.discovery.curator.path
    val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path  ===Services announce themselves under this ZooKeeper path.  zookeepr内部存储druid数据的路径
    //****对应json:dataSource
    val dataSource = "news2"   //Provide the location of your Druid dataSource    类似于数据库中的表，指定一个druid中的对应的数据块
    //****对应json:dimensions
    val dimensions = IndexedSeq("news_entry_id","country","language")   //呈现的列名,可以有多个
    //****对应json:spatialDimensions
    val spatialDimensions = Nil
    //****对应json: metricsSpec
    val aggregators = Seq(
      new LongSumAggregatorFactory("impression_cnt", "impression"),
      new LongSumAggregatorFactory("click_cnt", "click"),
      new CountAggregatorFactory("count")
    )   //进行汇聚的列baz

    // Expects simpleEvent.timestamp to return a Joda DateTime object.
    //下面的参数配置构成了tranquality使用的json数据的全部内容
    DruidBeams
      .builder[SimpleEvent]()(new Timestamper[SimpleEvent] {
      override def timestamp(a: SimpleEvent): DateTime = {
        val timestamp = new DateTime(a.timestamp)
        println(s"------timestamp-------${timestamp}")
        timestamp
      }
    })
      .curator(curator)
      .discoveryPath(discoveryPath)   //discovery znode
      .location(DruidLocation.create(indexService, dataSource))   //*********数据源信息


            .druidBeamConfig(new DruidBeamConfig(
            //以下为默认参数具体需要详细了解
            firehoseGracePeriod= new Period("PT5M"),
            firehoseQuietPeriod = new Period("PT1M"),
          firehoseRetryPeriod = new Period("PT1M"),
          firehoseChunkSize = 1000,
          randomizeTaskId= false,
          indexRetryPeriod= new Period("PT1M"),
          firehoseBufferSize = 100000,
          overlordLocator = OverlordLocator.Curator,
            //****对应json:druidBeam.taskLocator
          taskLocator = TaskLocator.Curator,
            //****对应json:druidBeam.overlordPollPeriod
          overlordPollPeriod= new Period("PT5S")
          ))



      //***********************DruidBeamConfig  设置与druid任务通信的参数,全部有默认值
      //.eventTimestamped(x=>new DateTime())
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.MINUTE))   //聚合操作
      .tuning(   //**************控制druidtask何时以及如何创建
      ClusteredBeamTuning(
        //****对应json:segmentGranularity
        segmentGranularity = Granularity.MINUTE,
        warmingPeriod=new Period("PT0M"), //
        //****对应json:windowPeriod
        windowPeriod = new Period("PT1M"),  //窗口期10分钟
        partitions = 1,
        replicants = 1,
        minSegmentsPerBeam=2,    //每个Beam最少多少个segment
        maxSegmentsPerBeam=10    //每个Beam最多多少个segment
      )
    )
      //设置数据中时间字段以及各式，missvalue针对数据源中没有时间字段的数据 进行设置的默认时间参数(默认为当前时间)
      .timestampSpec(new TimestampSpec("timestamp","auto",null))
      .buildBeam()
  }
}

