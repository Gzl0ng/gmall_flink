package com.gzl0ng.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
public class KeywordUtil {
    public static List<String> splitKeyWord(String keyWord) throws IOException {
        //创建集合用于存放结果集

        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        //true是尽可能小的切分，false是MaxWord方式
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);


        while (true){
            Lexeme next = ikSegmenter.next();
            if (next != null){
                String word = next.getLexemeText();
                resultList.add(word);
            }else {
                break;
            }
        }

        return resultList;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("斗罗大陆之绝世唐门"));
    }
}
