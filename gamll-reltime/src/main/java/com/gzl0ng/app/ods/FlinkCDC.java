package com.gzl0ng.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.gzl0ng.app.function.CustomerDeserialization;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CheckPonit并指定状态后端为FS  menory fs rocksdb
//        env.setStateBackend(new FsStateBackend("hdfs://flink1:8020/gmall-flink/ck"));
//        env.enableCheckpointing(5000);   //二次ck之间头之间的间隔,生产配置5分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);  //上一次ck结束后到第二次开始的时间,配置保存状态需要的时间
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));//默认是这个策略，固定延时重启,1.10版本重启次数是int的最大值,新版本重启次数比较合理

        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("flink1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall-flink")
//                .tableList("gmall-flink.base_trademark")  //如果不添加该参数，则消费指定数据库中所有表的数据，如果指定 库.表
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.打印数据并将数据写入kafka
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //4.启动任务
        env.execute("FlinkCDC");
    }

}
