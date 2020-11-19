package org.biubiu.cdc

import java.time.Duration

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


object FlinkMySqlCdc {
  def main(args: Array[String]): Unit = {
    // 初始化 stream 环境
    // 本地测试，需要 flink-runtime-web 依赖
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 失败重启,固定间隔,每隔3秒重启1次,总尝试重启10次
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3))

    // 本地测试线程 1
    //    env.setParallelism(1)

    // 事件处理的时间，由系统时间决定
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build

    // 创建 streamTable 环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    //    tableConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

    // checkpoint 设置
    val tableConfig = tableEnv.getConfig.getConfiguration
    // 开启checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的超时时间周期，1 分钟做一次checkpoint, 每次checkpoint 完成后 sink 才会执行
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60))
    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
    // tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
    // tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
    // 同一时间只允许进行一个检查点
    // tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
    // 手动cancel时是否保留checkpoint
   // tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置状态的最小空闲时间和最大的空闲时间
    // tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

    // Catalog
    //    val memoryCatalog = new GenericInMemoryCatalog("kafkaSourceTable", "memory")

    // 删除null数据
    //    tableEnv.getConfig.getConfiguration.setString("table.exec.sink.not-null-enforcer", "drop")

    val packageSourceSql =
      """
        |    CREATE TABLE t1(
        |    id int,
        |    country string

        |  ) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'localhost',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'root',
        |    'database-name' = 'flinkcdc',
        |    'table-name' = 't1'
        |)
      """.stripMargin


    tableEnv.executeSql(packageSourceSql)

    val insert =
      """
        |insert into t2
        |select country,count(*)  from t1 group by country
      """.stripMargin


    val printSinkSql =

      """
        | CREATE TABLE t2(
        | country string,
        |   cnt bigint,
        |   primary key(country)  NOT ENFORCED
        |  ) WITH (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/flinkcdc',
        |  'connector.table' = 't2',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = 'root',
        |  'connector.write.flush.max-rows' = '1'
        | )
      """.stripMargin


    tableEnv.executeSql(printSinkSql)
    tableEnv.executeSql(insert)

  }
}
