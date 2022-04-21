package com.gzl0ng.app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @author 郭正龙
 * @date 2022-04-11
 */
//数据流：web/app -> nginx -> springboot ->mysql -FlinkApp -> kafka(ods) -> FlinkApp -> kafka(dwd)/phoenix(dim) -> FlinkApp -> kafka(dwm)
//程序:             mockDb -> mysql -> flinkCDC -> kafka(zK) -> BaseDBApp -> kafka/phoenix(hbase,zk,hdfs) -> UniqueVisitApp -> kafka
public class UniqueVisitApp {
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


        //2.读取kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3.将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(JSONObject::parseObject);

        //4.过滤数据 状态编程 只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                //设置状态的超时时间以及更新时间的方式
                //在状态创建和写入的时候重新设置过期时间
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //取出上一条页面信息
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0){

                    //取出状态数据
                    String lastDate = dateState.value();
                    //取出今天的日期
                    String curDate = simpleDateFormat.format(jsonObject.getLong("ts"));
                    //判断二个日期是否相同
                    if (!curDate.equals(lastDate)){
                        dateState.update(curDate);
                        return true;
                    }
                }
                    return false;
            }
        });

        //5.将数据写入kafka
        uvDs.print();
        uvDs.map(JSONAware::toJSONString)
            .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //6.执行
        env.execute("UniqueVisitApp");
    }
}
