package com.gzl0ng.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author 郭正龙
 * @date 2022-04-13
 */
public interface DimAsyncJoinFunction<T> {
     String getKey(T input);

     void join(T input, JSONObject dimInfo) throws ParseException;

}
