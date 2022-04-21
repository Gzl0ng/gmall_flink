package com.gzl0ng.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
public class FlinkTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> textStream = env.socketTextStream("flink1", 9999);

        SingleOutputStreamOperator<String> wordDs = textStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDs = wordDs.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print().setParallelism(1);

        env.execute();
    }
}
