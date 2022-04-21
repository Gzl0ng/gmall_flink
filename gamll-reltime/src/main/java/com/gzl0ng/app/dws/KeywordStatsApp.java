package com.gzl0ng.app.dws;

import com.gzl0ng.app.function.SplitFunction;
import com.gzl0ng.bean.KeywordStats;
import com.gzl0ng.utils.ClickHouseUtil;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
//数据流：web/app -> nginx -> springBoot -> kafka(ods) ->flinkAPP -> kafka(dwd) -> flinkApp ->clickhouse
//程序:mockLog -> nginx -> logger.sh -> kafka(zK) -> BaseLogApp -> kafka -> KeywordStatsApp -> clickhouse
public class KeywordStatsApp {
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

        //TODO 2.使用ddl方式读取kafka数据创建表
        String sourceTopic = "dwd_page_log";
        String groupId = "keyword_stats_app";
        tableEnv.executeSql("create table page_view(" +
                "`common` Map<STRING,STRING>," +
                "`page` Map<STRING,STRING>," +
                "`ts` BIGINT," +
                "`rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND" +
                ") with (" +
                MyKafkaUtil.getKafkaDDL(sourceTopic,groupId) + ")");

        //TODO 3.过滤数据 上一跳页面为“search” and搜索词 is not null
        Table fullWordTable = tableEnv.sqlQuery("select " +
                "page['item'] full_word," +
                "rt from page_view " +
                "where " +
                "page['last_page_id'] ='search' and page['item'] is not null");

        //TODO 4.注册udtf，进行分词处理
        tableEnv.createTemporaryFunction("split_words", SplitFunction.class);
        Table wordTable = tableEnv.sqlQuery("select " +
                "word, " +
                "rt " +
                "FROM " + fullWordTable +
                ",LATERAL TABLE(split_words(full_word))");

        //TODO 5.分组开窗，聚合
        Table resultTable = tableEnv.sqlQuery("select " +
                "'search' source," +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt," +
                "word keyword," +
                "count(*) ct," +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from " + wordTable +
                " group by word,TUMBLE(rt,INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7.将数据打印并写入clickhosue
        keywordStatsDataStream.print(">>>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.<KeywordStats>getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
