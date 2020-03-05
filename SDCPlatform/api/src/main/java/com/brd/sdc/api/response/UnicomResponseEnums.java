package com.brd.sdc.api.response;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName UnicomResponseEnums.java
 * @Description 友好提示枚举
 * @createTime 2020年02月28日 17:28:00
 */
public enum UnicomResponseEnums {
    SUCCESS("1","success"),
    FALSE("0","false");

    private String code;
    private String msg;
    private String ss;
    private UnicomResponseEnums(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    public String getCode() {
        return code;
    }
    public void setCode(String code) {
        this.code = code;
    }
    public String getMsg() {
        return msg;
    }
    public void setMsg(String msg) {
        this.msg = msg;
    }
}
