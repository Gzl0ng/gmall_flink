package com.gzl0ng.gmallpublishertest.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
@Service
public interface SugarService {
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date,int limit);

//    Map getGmvByC3(int date,int limit);
//
//    Map getGmvBySpu(int date,int limit);
}
