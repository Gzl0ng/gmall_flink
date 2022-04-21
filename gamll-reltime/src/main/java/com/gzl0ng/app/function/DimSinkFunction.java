package com.gzl0ng.app.function;

import com.alibaba.fastjson.JSONObject;
import com.gzl0ng.common.GmallConfig;
import com.gzl0ng.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author 郭正龙
 * @date 2022-04-10
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:
    //SQL:upsert into db.dn(id,tm_name) values(...,...)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable,after
                    );

            System.out.println("写入hbase的语句：" + upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //判断如果当前数据为更新操作，则先删除Redis中的数据
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(),after.getString("id"));
            }

            //执行插入操作,可以批处理
            preparedStatement.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    //after:
    //sql:upsert into db.tn(id,tm_name) values(....,....)
    private String genUpsertSql(String sinkTable, JSONObject after) {
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();

        //keySet.mkString(","); => "id,tm_name"

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet,",") + ") values('" +
                StringUtils.join(values,"','") + "')";
    }
}
