package com.gzl0ng.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.gzl0ng.app.function.CustomerDeserialization;
import com.gzl0ng.app.function.DimSinkFunction;
import com.gzl0ng.app.function.TableProcessFunction;
import com.gzl0ng.bean.TableProcess;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */

//数据流：web/app -> nginx -> springboot ->mysql -FlinkApp -> kafka(ods) -> FlinkApp -> kafka(dwd)/phoenix(dim)
//程序:             mockDb -> mysql -> flinkCDC -> kafka(zK) -> BaseDBApp -> kafka/phoenix(hbase,zk,hdfs)
public class BaseDbApp {
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

        //2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3.将每行数据转换为JSON对象并过滤（delete）   主流
        SingleOutputStreamOperator<JSONObject> JsonObjDs = kafkaDs.map(JSONObject::parseObject).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //取出数据的操作类型
                String type = jsonObject.getString("type");

                return !"delete".equals(type);
            }
        });

        //4.使用flinkCDC消费配置表并处理成     广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("flink1")
                .password("3306")
                .username("root")
                .password("root")
                .databaseList("gmall-realtime")  //生产环境用的库不在业务数据库，会单独弄一个
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDs = env.addSource(sourceFunction);
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<>("table-state",String.class,TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDs.broadcast(mapStateDescriptor);

        //5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = JsonObjDs.connect(broadcastStream);

        //6.处理数据 广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));

        //7.提取kafka流数据和hbase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        //8.将kafka数据写入kafka主题，将hbase数据写入Phoenix表
        kafka.print("kafka>>>>>");
        hbase.print("hbase>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes());
            }
        }));

        //9.启动任务
        env.execute("BaseDbApp");
    }
}
