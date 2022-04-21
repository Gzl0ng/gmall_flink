package com.gzl0ng.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.bean.OrderWide;
import com.gzl0ng.bean.PaymentInfo;
import com.gzl0ng.bean.PaymentWide;
import com.gzl0ng.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author 郭正龙
 * @date 2022-04-13
 */
//数据流 web/app -> nginx -> springBoot -> mysql -> flinkApp -> kafka(ods) -> flinkApp -> kafka/phoenix(dwd-dim) - FlinkApp(redis) -> kafka(dwm) -> FlinkApp -> kafka(dwm)
//程序 mockDb -> mysql -> FlinkCDC -> kafka(zk) -> BaseDbApp -> kafka/phoenix(zk,hdfs,hbase) -> orderWideApp(redis) -> kafka -> paymentWideApp -> kafka
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取kafka主题的数据创建流 并转换为JavaBean对象 并提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<OrderWide> orderWideDs = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> {
                    return JSONObject.parseObject(line, OrderWide.class);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDs = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> {
                    return JSONObject.parseObject(line, PaymentInfo.class);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        //3.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDs = paymentInfoDs.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDs.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        //4.将数据写入kakfa
        paymentWideDs.print(">>>>>>>>>");
        paymentWideDs.
                map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        //5.启动任务
        env.execute("PaymentWideApp");
    }
}
