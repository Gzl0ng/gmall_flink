package com.gzl0ng.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.bean.VisitorStats;
import com.gzl0ng.utils.ClickHouseUtil;
import com.gzl0ng.utils.DateTimeUtil;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author 郭正龙
 * @date 2022-04-14
 */
//数据流：web/app -> nginx -> springBoot -> kafka(ods) ->flinkAPP -> kafka(dwd) -> FlinkApp -> kafka(dws) -> flinkApp -> clickhosue
//程序:mockLog -> nginx -> logger.sh -> kafka(zK) -> BaseLogApp -> kafka -> uvApp/ujApp -> kafka -> VisitorStatsApp -> ClickHouse
public class VisitorStatsApp {
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


        //2.读取kafka数据创建流
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> uvDs = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDs = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDs = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //3.将每个流处理成相同的数据类型
        //3.1处理uv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDs = uvDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });
            //3.2处理uj数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDs = ujDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });
        //3.3处理pv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDs = pvDs.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);
            //获取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            //获取页面信息
            JSONObject page = jsonObject.getJSONObject("page");

            String last_page_id = page.getString("last_page_id");

            long sv = 0L;

            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));

        });

        //4.Union几个流
        DataStream<VisitorStats> unionDs = visitorStatsWithUvDs.union(
                visitorStatsWithUjDs,
                visitorStatsWithPvDs);

        //5.提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStats> vistorStatsWithWMDs = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //6.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = vistorStatsWithWMDs.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc());
            }
        });

        //7.开窗聚合 10s的滚动窗口,如果是滑动窗口只能用new的方式赋值，因为一个value1属于多个窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
//               new VisitorStats(
//                        value1.getStt(),
//                        value1.getEdt(),
//                        value1.getVc(),
//                        value1.getCh(),
//                        value1.getAr(),
//                        value1.getIs_new(),
//                        value1.getUv_ct()+value2.getUv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();
                VisitorStats next = input.iterator().next();

                //补充窗口信息
                next.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                next.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(next);
            }
        });

        //8.将数据写入clickhouse
        result.print(">>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9.任务启动
        env.execute("VisitorStatsApp");
    }
}
