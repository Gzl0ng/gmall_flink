package com.gzl0ng.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */

//数据流：web/app -> nginx -> springBoot -> kafka(ods) ->flinkAPP -> kafka(dwd)
//程序:mockLog -> nginx -> logger.sh -> kafka(zK) -> BaseLogApp -> kafka
public class BaseLogApp {
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


        //2.消费ods_base_log  主题数据流创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3.将每行数据转换为JSON对象
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDs.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常，将数据写入侧输出流
                    context.output(outputTag, s);
                }
            }
        });

        //打印脏数据
        jsonObjectDS.getSideOutput(outputTag).print("Dirty>>>>");

        //4.新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDs = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {

                        //获取数据中的“is_new”字段
                        String isNew = jsonObject.getJSONObject("common").getString("is_new ");

                        //判断isNew标记是否为“1”
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                //修改isNew标记
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });

        //5.分流  侧输出流 页面：主流  启动：侧输出1流  曝光：侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDs = jsonObjWithNewFlagDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                //获取启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startOutputTag, jsonObject.toJSONString());
                } else {
                    //将数据写入页面日志
                    collector.collect(jsonObject.toJSONString());

                    //取出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //添加页面id
                            display.put("page_id", pageId);
                            //将输出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }

            }
        });

        //6.提取侧输出流
        DataStream<String> startDs = pageDs.getSideOutput(startOutputTag);
        DataStream<String> displayDs = pageDs.getSideOutput(displayTag);

        //7.将三个流进行打印并输出到对应的kafka主题中
        startDs.print("Start>>>>>>>>>");
        pageDs.print("Page>>>>>>>>>");
        displayDs.print("Display>>>>>>>");

        startDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //8.启动任务
        env.execute("BaseLogApp");

    }
}
