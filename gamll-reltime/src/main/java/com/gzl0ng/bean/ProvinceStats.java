package com.gzl0ng.bean;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author 郭正龙
 * @date 2022-04-17
 */
public class ProvinceStats {
    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private BigDecimal order_amount;
    private Long	order_count;
    private Long ts;

    public ProvinceStats(OrderWide orderWide){ province_id = orderWide.getProvince_id();
        order_amount = orderWide.getSplit_total_amount(); province_name=orderWide.getProvince_name();
        province_area_code=orderWide.getProvince_area_code(); province_iso_code=orderWide.getProvince_iso_code();
        province_3166_2_code=orderWide.getProvince_3166_2_code();

        order_count = 1L; ts=new Date().getTime();
    }

    public ProvinceStats() {
    }

    public ProvinceStats(String stt, String edt, Long province_id, String province_name, String province_area_code, String province_iso_code, String province_3166_2_code, BigDecimal order_amount, Long order_count, Long ts) {
        this.stt = stt;
        this.edt = edt;
        this.province_id = province_id;
        this.province_name = province_name;
        this.province_area_code = province_area_code;
        this.province_iso_code = province_iso_code;
        this.province_3166_2_code = province_3166_2_code;
        this.order_amount = order_amount;
        this.order_count = order_count;
        this.ts = ts;
    }

    public String getStt() {
        return stt;
    }

    public void setStt(String stt) {
        this.stt = stt;
    }

    public String getEdt() {
        return edt;
    }

    public void setEdt(String edt) {
        this.edt = edt;
    }

    public Long getProvince_id() {
        return province_id;
    }

    public void setProvince_id(Long province_id) {
        this.province_id = province_id;
    }

    public String getProvince_name() {
        return province_name;
    }

    public void setProvince_name(String province_name) {
        this.province_name = province_name;
    }

    public String getProvince_area_code() {
        return province_area_code;
    }

    public void setProvince_area_code(String province_area_code) {
        this.province_area_code = province_area_code;
    }

    public String getProvince_iso_code() {
        return province_iso_code;
    }

    public void setProvince_iso_code(String province_iso_code) {
        this.province_iso_code = province_iso_code;
    }

    public String getProvince_3166_2_code() {
        return province_3166_2_code;
    }

    public void setProvince_3166_2_code(String province_3166_2_code) {
        this.province_3166_2_code = province_3166_2_code;
    }

    public BigDecimal getOrder_amount() {
        return order_amount;
    }

    public void setOrder_amount(BigDecimal order_amount) {
        this.order_amount = order_amount;
    }

    public Long getOrder_count() {
        return order_count;
    }

    public void setOrder_count(Long order_count) {
        this.order_count = order_count;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ProvinceStats{" +
                "stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", province_id=" + province_id +
                ", province_name='" + province_name + '\'' +
                ", province_area_code='" + province_area_code + '\'' +
                ", province_iso_code='" + province_iso_code + '\'' +
                ", province_3166_2_code='" + province_3166_2_code + '\'' +
                ", order_amount=" + order_amount +
                ", order_count=" + order_count +
                ", ts=" + ts +
                '}';
    }
}

