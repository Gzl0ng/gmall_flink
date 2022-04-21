package com.gzl0ng.gmallpublishertest.service.impl;

import com.gzl0ng.gmallpublishertest.mapper.ProductStatsMapper;
import com.gzl0ng.gmallpublishertest.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
@Service
public class SugarServiceImpl implements SugarService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }

    /**List[
     *Map[("tm_name"->"苹果"),(order_amount->3153)]
     * Map[("tm_name"->"华为"),(order_amount->3153)]
     * Map[("tm_name"->"TCL"),(order_amount->3153)]
     * Map[("tm_name"->"小米"),(order_amount->3153)]
     * ]
     *
     * ==>
     *
     * Map[("苹果"->3153),("华为"->3153),.....]
     */
    @Override
    public Map getGmvByTm(int date, int limit) {

        //查询数据
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        //创建Map用于存放结果数据
        HashMap<String, BigDecimal> result = new HashMap<>();

        //遍历maplist，将数据取出放入result Map[("tm_name"->"苹果"),(order_amount->165456)]
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"),(BigDecimal) map.get("order_amount"));
        }

        return result;
    }

    /**List[
     *Map[("category3_id"->"1"),("category3_name"->手机),("order_amount"->315351)]
     * Map[("category3_id"->"2"),("category3_name"->平板电视),("order_amount"->315351)]
     * Map[("category3_id"->"3"),("category3_name"->唇部),("order_amount"->315351)]
     * Map[("category3_id"->"4"),("category3_name"->香水),("order_amount"->315351)]
     * ]
     *
     * ==>
     *
     * Map[("category3_name"->手机),("order_amount"->315351),.....]
     */
//    @Override
//    public Map getGmvByC3(int date, int limit) {
//        //查询数据
//        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);
//
//        //创建Map用于存放结果数据
//        HashMap<String, BigDecimal> result = new HashMap<>();
//
//        //遍历maplist，将数据取出放入result Map[("tm_name"->"苹果"),(order_amount->165456)]
//        for (Map map : mapList) {
//            result.put((String) map.get("category3_name"),(BigDecimal) map.get("order_amount"));
//        }
//
//        return result;
//    }
//
//
//
//    @Override
//    public Map getGmvBySpu(int date, int limit) {
//        //查询数据
//        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);
//
//        //创建Map用于存放结果数据
//        HashMap<String, BigDecimal> result = new HashMap<>();
//
//        //遍历maplist，将数据取出放入result Map[("tm_name"->"苹果"),(order_amount->165456)]
//        for (Map map : mapList) {
//            result.put((String) map.get("tm_name"),(BigDecimal) map.get("order_amount"));
//        }
//
//        return result;
//    }
}
