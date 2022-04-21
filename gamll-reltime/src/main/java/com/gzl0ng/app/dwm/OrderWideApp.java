package com.gzl0ng.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.app.function.DimAsyncFunction;
import com.gzl0ng.bean.OrderDetail;
import com.gzl0ng.bean.OrderInfo;
import com.gzl0ng.bean.OrderWide;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author 郭正龙
 * @date 2022-04-12
 */
//数据流 web/app -> nginx -> springBoot -> mysql -> flinkApp -> kafka(ods) -> flinkApp -> kafka/phoenix(dwd-dim) - FlinkApp(redis) -> kafka(dwm)
//程序 mockDb -> mysql -> FlinkCDC -> kafka(zk) -> BaseDbApp -> kafka/phoenix(zk,hdfs,hbase) -> orderWideApp(redis) -> kafka
public class OrderWideApp {
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


        //2.读取kafka 主题的数据 并转换为javabean对象 & 提取时间戳生成watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        SingleOutputStreamOperator<OrderInfo> orderInfoDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSONObject.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        //3.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDs = orderInfoDs.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDs.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) //生产环境中给的时间为最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        orderWideWithNoDimDs.print("orderWideWithNoDimDs");

        //4.关联维度信息  HBase phoenix
//        orderWideWithNoDimDs.map(orderWide->{
            //关联用户维度
//            Long user_id = orderWide.getUser_id();
            //根据user_id查询phoenix用户信息
            //根据用户信息补充至orderWide     地区 SKU SPU ......
            //返回结果
//            return orderWide;
//        });

        //4.1关联用户维度:因为zookeeper超时时间为60秒，所以最少大于60秒
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDs = AsyncDataStream.unorderedWait(orderWideWithNoDimDs,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setUser_gender(dimInfo.getString("GENDER"));
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                        input.setUser_age((int)age);
                    }
                },
                60,
                TimeUnit.SECONDS);
//        orderWideWithUserDs.print("orderWideWithUserDs");

        //4.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvince = AsyncDataStream.unorderedWait(orderWideWithUserDs,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);

        //4.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDs = AsyncDataStream.unorderedWait(orderWideWithProvince,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //4.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSkuDs, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //4.5关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //4.6关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
                AsyncDataStream.unorderedWait(
                        orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS");


        //5.将数据写入kafka
        orderWideWithCategory3DS.
                map(orderWide -> JSONObject.toJSONString(orderWide))
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //6.启动任务
        env.execute("OrderWideApp");
    }
}
