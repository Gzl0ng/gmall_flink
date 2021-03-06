package com.gzl0ng.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.app.function.DimAsyncFunction;
import com.gzl0ng.bean.OrderWide;
import com.gzl0ng.bean.PaymentWide;
import com.gzl0ng.bean.ProductStats;
import com.gzl0ng.common.GmallConstant;
import com.gzl0ng.utils.ClickHouseUtil;
import com.gzl0ng.utils.DateTimeUtil;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author 郭正龙
 * @date 2022-04-16
 */
//数据流:
// app/web -> nginx -> springBoot -> kafka(ods) -> flinkApp -> kafka(dwd) -> flinkApp -> clickHuse
//app/web -> nginx -> springBoot -> mysql -> flinkApp -> kafka(ods) -> flinkApp -> kafka/phoenix -> flinkApp
//  -> kafka(dwm) -> flinkApp -> clickHouse
//程序:mock -> ngninx -> logger.sh -> kafka(ZK)/phoenix(hdfs/hbase/ZK) -> redis -> clickHouse
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO  1.获取执行环境
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

        //TODO  2.读取kafka7个主题的数据创建流
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDs = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDs = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDs = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDs = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refunDs = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commontDs = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));


        //TODO  3.将7个流统一数据格式
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplay = pvDs.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                //将数据转换为json对象
                JSONObject jsonObject = JSONObject.parseObject(value);

                //取出page信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");

                Long ts = jsonObject.getLong("ts");

                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //尝试取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {

                        //取出单条曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDs = favorDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDs = cartDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDs = orderDs.map(line -> {
            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDs = payDs.map(line -> {
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);

            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDs = refunDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDs = commontDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //TODO  4.union7个流
        DataStream<ProductStats> unionDs = productStatsWithClickAndDisplay.union(
                productStatsWithFavorDs,
                productStatsWithCartDs,
                productStatsWithOrderDs,
                productStatsWithPaymentDs,
                productStatsWithRefundDs,
                productStatsWithCommentDs
        );

        //TODO  5.提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDs = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness
                (Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO  6.分组,开窗,聚合    按照sku_id分组，10秒的滚动窗口，结合增量聚合（累加值）和全量聚合(提取窗口信息)
        SingleOutputStreamOperator<ProductStats> reduceDs = productStatsWithWMDs.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
//                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
//                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);

                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
//                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //设置窗口时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //设置订单数量
                        productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                        productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                        productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);


                        //将数据写出
                        out.collect(productStats);

                    }
                });

        //TODO  7.关联维度信息

        //7.1关联sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSKUDs = AsyncDataStream.unorderedWait(reduceDs,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {

                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));

                    }
                }, 60, TimeUnit.SECONDS);

        //7.2关联spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDs = AsyncDataStream.unorderedWait(productStatsWithSKUDs,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {

                        return String.valueOf(input.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));

                    }
                }, 60, TimeUnit.SECONDS);


        //7.3关联category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Ds = AsyncDataStream.unorderedWait(productStatsWithSpuDs, new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(ProductStats input) {
                return String.valueOf(input.getCategory3_id());
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                input.setCategory3_name(dimInfo.getString("NAME"));

            }
        }, 60, TimeUnit.SECONDS);

        //7.4关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDs = AsyncDataStream.unorderedWait(productStatsWithCategory3Ds,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getTm_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setTm_name(dimInfo.getString("TM_NAME"));

                    }
                }, 60, TimeUnit.SECONDS);

        //TODO  8.将数据写入clickhouse
        productStatsWithTmDs.addSink(ClickHouseUtil.getSink("insert into table product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO  9.启动任务
        env.execute("ProductStatsApp");
    }
}
