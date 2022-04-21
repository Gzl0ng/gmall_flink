package com.gzl0ng.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
public class FlinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        ParameterTool systemProperties = ParameterTool.fromSystemProperties();

        //顺序不能错  flink1 9999
        DataStreamSource<String> stream1 = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        // --host flink1 --port 9999
        DataStreamSource<String> stream2 = env.socketTextStream(host, port);
    }
}
