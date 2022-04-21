package com.gzl0ng.gmallpublishertest.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
//一张表一个mapper

public interface ProductStatsMapper {

    //select sum(order_amount) from product_stats where toYYYYMMDD(stt)=20220417;
    @Select("select sum(order_amount) from product_stats where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGmv(int date);

    /**
     *Map[("tm_name"->"苹果"),(order_amount->3153)]  => [("苹果"->21655),....]
     */
    //select tm_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=20220417 group by tm_name order by order_amount desc limit 5;
    @Select("select tm_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = #{date} group by tm_name order by order_amount desc limit #{limit};")
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);

//    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount > 0 order by order_amount desc limit #{limit};")
//    public List<Map> selectGmvByC3(@Param("date")int date , @Param("limit") int limit);
//
//    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(product_stats.order_ct) order_ct from product_stats where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name having order_amount > 0 order by order_amount desc limit #{limit};")
//    public List<Map> selectGmvBySpu(@Param("date") int date, @Param("limit") int limit);

}
