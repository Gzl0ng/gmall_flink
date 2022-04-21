package com.gzl0ng;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2022-04-07
 */
public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.DDL方式建表
        tableEnv.executeSql("CREATE TABLE mysql_binlog ( " +
                " id STRING NOT NULL, " +
                " tm_name STRING, " +
                " logo_url STRING" +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'flink1', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = 'root', " +
                " 'database-name' = 'gmall-flink', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        //3.查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4.将动态表转换为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5.启动任务
        env.execute("FlinkCDCWithSQL");
    }

}
