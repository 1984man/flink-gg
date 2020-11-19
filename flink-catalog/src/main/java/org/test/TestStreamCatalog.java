package org.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class TestStreamCatalog {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "flinkstream";
        String hiveConfDir     = "/opt/rt/hive/conf"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

        String sql = "CREATE TABLE mykafka (name String, age Int) WITH (\n" +
                "   'connector.type' = 'kafka',\n" +
                "   'connector.version' = 'universal',\n" +
                "   'connector.topic' = 'test',\n" +
                "   'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "   'format.type' = 'csv',\n" +
                "   'update-mode' = 'append'\n" +
                ")";

        tableEnv.sqlQuery(sql);


    }
}
