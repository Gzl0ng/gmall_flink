package com.gzl0ng.app.function;

import com.gzl0ng.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
//注解里word为列的默认名字，string为字段类型
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {

        try {
            //分词
            List<String> words = KeywordUtil.splitKeyWord(str);
            //遍历写出
            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
