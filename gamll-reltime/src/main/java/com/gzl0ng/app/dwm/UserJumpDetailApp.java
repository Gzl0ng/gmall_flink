package com.gzl0ng.app.dwm;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 郭正龙
 * @date 2022-04-11
 */
//数据流：web/app -> nginx -> springBoot -> kafka(ods) ->flinkAPP -> kafka(dwd) -> FlinkApp -> kafka(dwm)
//程序:mockLog -> nginx -> logger.sh -> kafka(zK) -> BaseLogApp -> kafka -> UserJumpDetailApp -> kafka
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境，与kafka分区数保持一致
//1.1 开启CheckPonit并指定状态后端为FS  menory fs rocksdb
//        env.setStateBackend(new FsStateBackend("hdfs://flink1:8020/gmall-flink/ck"));
//        env.enableCheckpointing(5000);   //二次ck之间头之间的间隔,生产配置5分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);  //上一次ck结束后到第二次开始的时间,配置保存状态需要的时间
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));//默认是这个策略，固定延时重启,1.10版本重启次数是int的最大值,新版本重启次数比较合理

        //2.读取akfka主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,groupId));

        //3.将每行数据转换为JSON对象并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(JSONObject::parseObject).
                assignTimestampsAndWatermarks(WatermarkStrategy.
                        <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //  4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() == 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() == 0;
            }
        }).within(Time.seconds(10));

        //使用循环模式 定义模式序列
       Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
           @Override
           public boolean filter(JSONObject value) throws Exception {
               String lastPageId = value.getJSONObject("page").getString("last_page_id");
               return lastPageId == null || lastPageId.length() <= 0;

           }
       }).times(2)
               .consecutive()  //指定严格近邻(next)
               .within(Time.seconds(10));

        //  5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjDs.keyBy(json -> json.getJSONObject("common").getString("mid"))
                , pattern);

        //  6.提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDs = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeOutDs = selectDs.getSideOutput(timeOutTag);

        //7.UNION二种事件
        DataStream<JSONObject> unionDs = selectDs.union(timeOutDs);

        //8.将数据写入kafka
        unionDs.print();
        unionDs.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //9.启动任务
        env.execute("UserJumpDetailApp");
    }
}
