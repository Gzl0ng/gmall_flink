package com.gzl0ng.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.gzl0ng.bean.TableProcess;
import com.gzl0ng.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //这里可以做改进，在这里使用jdbc读取一次配置表Tableproces写入map，这样先启动flinkCDC也不会丢数据
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
        System.out.println("广播key：" + key);
    }

    //建表语句:create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
        if (sinkPk == null){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        StringBuffer createTableSQl = new StringBuffer("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];

            //判断是否为主键
            if (sinkPk.equals(field)){
                createTableSQl.append(field).append(" varchar primary key");
            }else {
                createTableSQl.append(field).append(" varchar");
            }

            //判断是否为最后一个字段，如果不是，则添加逗号","
            if (i < fields.length-1){
                createTableSQl.append(",");
            }
        }

        createTableSQl.append(")").append(sinkExtend);

        //打印建表语句
        System.out.println(createTableSQl);

        //预编译SQL

            preparedStatement = connection.prepareStatement(createTableSQl.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败!");
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null){

            //2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //将输出表/主题写入value
            value.put("sinkType",tableProcess.getSinkType());
            value.put("sinkTable",tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                //kafka数据，写入主流
                out.collect(value);
            }else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                //hbase数据，写入侧输出流
                ctx.output(objectOutputTag,value);
            }
        }else {
            System.out.println("该组合key:" + key + "不存在");
        }
    }

    /**
     * {"id":"11","tm_name":"atguigu","log_url":"aaa"}
     * id,tm_name
     * {"id":"11","tm_name":"atguigu"}
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (! columns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }

        data.entrySet().removeIf(next -> ! columns.contains(next.getKey()));

    }
}
