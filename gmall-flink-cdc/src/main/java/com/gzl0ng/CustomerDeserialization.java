package com.gzl0ng;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author 郭正龙
 * @date 2022-04-07
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before",{"id":"tm_name","":""....},
     * "after":{"":"","":""....},
     * "type","c u d",
     * //"ts":156532113551
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //1.创建json对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();
        //3.获取“before”数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field beforeField : beforeFields) {
                Object beforeValue = before.get(beforeField);
                beforeJson.put(beforeField.name(), beforeValue);
            }
        }

        //4.获取“after”数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        //5.获取操作类型 CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        //6.将字段写入json对象
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
