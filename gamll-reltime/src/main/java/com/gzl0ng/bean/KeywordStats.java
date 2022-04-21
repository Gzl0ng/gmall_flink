package com.gzl0ng.bean;

/**
 * @author 郭正龙
 * @date 2022-04-18
 */
public class KeywordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;

    public KeywordStats() {
    }

    public KeywordStats(String keyword, Long ct, String source, String stt, String edt, Long ts) {
        this.keyword = keyword;
        this.ct = ct;
        this.source = source;
        this.stt = stt;
        this.edt = edt;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "KeywordStats{" +
                "keyword='" + keyword + '\'' +
                ", ct=" + ct +
                ", source='" + source + '\'' +
                ", stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", ts=" + ts +
                '}';
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public Long getCt() {
        return ct;
    }

    public void setCt(Long ct) {
        this.ct = ct;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
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

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
