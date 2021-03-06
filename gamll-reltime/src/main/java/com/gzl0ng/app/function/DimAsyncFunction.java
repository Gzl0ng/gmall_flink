package com.gzl0ng.app.function;

import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.common.GmallConfig;
import com.gzl0ng.utils.DimUtil;
import com.gzl0ng.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 郭正龙
 * @date 2022-04-13
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncJoinFunction<T>{
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;



    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadpool();
    }

     @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取查询的主键
                    String id = getKey(input);

                    //查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                    //补充维度信息
                    if (dimInfo != null){
                        join(input,dimInfo);
                    }

                    //将数据输出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }



    //生产环境可以设置超时重试
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
