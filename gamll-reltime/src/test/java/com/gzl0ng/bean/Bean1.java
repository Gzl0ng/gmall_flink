package com.gzl0ng.bean;

/**
 * @author 郭正龙
 * @date 2022-04-12
 */
public class Bean1 {
    public Bean1() {
    }

    @Override
    public String toString() {
        return "Bean1{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", ts=" + ts +
                '}';
    }

    public Bean1(String id, String name, Long ts) {
        this.id = id;
        this.name = name;
        this.ts = ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    private String id;
    private String name;
    private Long ts;
}
