package com.gzl0ng.gmallpublishertest.controller;

import com.alibaba.fastjson.JSON;
import com.gzl0ng.gmallpublishertest.service.SugarService;
import com.gzl0ng.gmallpublishertest.service.impl.SugarServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
@RequestMapping("/api/sugar/")
@RestController
public class SugarController {

    @Autowired
    private SugarService sugarService;

//    @RequestMapping("c3")
//    public String getGmvByC3(@RequestParam(value = "date",defaultValue = "0") int date,
//                             @RequestParam(value = "limit",defaultValue = "5") int limit){
//        if (date == 0){
//            date = getToday();
//        }
//        Map gmvByC3 = sugarService.getGmvByC3(date, limit);
//        Set set = gmvByC3.keySet();
//        Collection values = gmvByC3.values();
//
//
//
//        return "{  " +
//                "  \"status\": 0,  " +
//                "  \"msg\": \"\",  " +
//                "  \"data\": [  " +
//                "    {  " +
//                "      \"name\": \"PC\",  " +
//                "      \"value\": 97,  " +
//                "      \"url\": \"http://www.baidu.com\"  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"iOS\",  " +
//                "      \"value\": 50,  " +
//                "      \"url\": \"http://www.baidu.com\"  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"Android\",  " +
//                "      \"value\": 59,  " +
//                "      \"url\": \"http://www.baidu.com\"  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"windows phone\",  " +
//                "      \"value\": 29  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"Black berry\",  " +
//                "      \"value\": 3  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"Nokia S60\",  " +
//                "      \"value\": 2  " +
//                "    },  " +
//                "    {  " +
//                "      \"name\": \"Nokia S90\",  " +
//                "      \"value\": 1  " +
//                "    }  " +
//                "  ]  " +
//                "}";
//    }

    @RequestMapping("tm")
    public String getGmvByTm(@RequestParam(value = "date",defaultValue = "0") int date,
                             @RequestParam(value = "limit",defaultValue = "5") int limit){

        if (date == 0){
            date = getToday();
        }
        Map gmvByTm = sugarService.getGmvByTm(date, limit);
        Set keySet = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\":  [\"" +
                StringUtils.join(keySet,"\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"商品品牌\", " +
                "        \"data\":  [\"" +
               StringUtils.join(values,"\",\"")+
                "\"]" +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }

    @RequestMapping("gmv")
    public String getGmv(@RequestParam(value = "date",defaultValue = "0") int date){
        if (date == 0){
            date = getToday();
        }

        HashMap<String, Object> result = new HashMap<>();
        result.put("status",0);
        result.put("msg",0);
        result.put("data",sugarService.getGmv(date));

//        return "{" + "\"status\":0," +
//                "\"msg\":\"\"," +
//                "\"data\":"+ sugarService.getGmv(date) +
//                "}";

        return JSON.toJSONString(result);
    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateTime = sdf.format(System.currentTimeMillis());

        return Integer.parseInt(dateTime);
    }

}
