package com.gzl0ng.bean;

/**
 * @author 郭正龙
 * @date 2022-04-12
 */
public class Bean2 {
    public Bean2() {
    }

    @Override
    public String toString() {
        return "Bean2{" +
                "id='" + id + '\'' +
                ", sex='" + sex + '\'' +
                ", ts=" + ts +
                '}';
    }

    public Bean2(String id, String sex, Long ts) {
        this.id = id;
        this.sex = sex;
        this.ts = ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    private String id;
    private String sex;
    private Long ts;
}
