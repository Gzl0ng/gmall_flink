package com.gzl0ng.common;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */
public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:flink1,flink2,flink3:2181";

    //CLICKHOUSE_URL
    public static final String
            CLICKHOUSE_URL = "jdbc:clickhouse://flink1:8123/default";
    //CLICKHOUSE_DRIVER
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

}
