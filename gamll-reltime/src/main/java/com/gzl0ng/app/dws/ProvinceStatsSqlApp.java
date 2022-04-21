package com.gzl0ng.app.dws;

import com.gzl0ng.bean.ProvinceStats;
import com.gzl0ng.utils.ClickHouseUtil;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 郭正龙
 * @date 2022-04-17
 */

//数据流 web/app -> nginx -> springBoot -> mysql -> flinkApp -> kafka(ods) -> flinkApp -> kafka/phoenix(dwd-dim) - FlinkApp(redis) -> kafka(dwm) -> flinkApp -> clickhouse
//程序 mockDb -> mysql -> FlinkCDC -> kafka(zk) -> BaseDbApp -> kafka/phoenix(zk,hdfs,hbase) -> orderWideApp(redis) -> kafka -> ProvinceStatsSqlApp ->clickhouse
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.1 开启CheckPonit并指定状态后端为FS  menory fs rocksdb
//        env.setStateBackend(new FsStateBackend("hdfs://flink1:8020/gmall-flink/ck"));
//        env.enableCheckpointing(5000);   //二次ck之间头之间的间隔,生产配置5分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);  //上一次ck结束后到第二次开始的时间,配置保存状态需要的时间
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));//默认是这个策略，固定延时重启,1.10版本重启次数是int的最大值,新版本重启次数比较合理

        //TODO 2.使用DDL创建表 提取时间戳生成WaterMark
        String sourceTopic = "dwm_order_wide";
        String groupId = "province_stats";
        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "`province_id` BIGINT, " +
                "`province_name` STRING, " +
                "`province_area_code` STRING, " +
                "`province_iso_code` STRING, " +
                "`province_3166_2_code` STRING, " +
                "`order_id` BIGINT, " +
                "`split_total_amount` DECIMAL, " +
                "`create_time` STRING, " +
                "`rt` as TO_TIMESTAMP(create_time), " +
                "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) with( " +
                MyKafkaUtil.getKafkaDDL(sourceTopic, groupId) + ")");

        //TODO 3.查询数据 分组，开窗，聚合
        Table table = tableEnv.sqlQuery(" select " +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                "province_id, " +
                "province_name, " +
                "province_area_code, " +
                "province_iso_code, " +
                "province_3166_2_code, " +
                "sum(split_total_amount) order_amount, " +
                "count(distinct order_id) order_count, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by " +
                "province_id, " +
                "province_name, " +
                "province_area_code, " +
                "province_iso_code, " +
                "province_3166_2_code, " +
                "TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5.打印并写入clickhouse
        provinceStatsDataStream.print(">>>>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into table province_stats values(?,?,?,?,?,?,?,?,?,?)"));


        //TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");
    }
}
