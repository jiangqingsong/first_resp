package com.brd.sdc.api.response;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName ResponseBean.java
 * @Description 返回的JSON数据结构标准
 * @createTime 2020年02月28日 17:26:00
 */
public class ResponseBean<T> {
    private String result;
    private String msg;
    private T data;

    public ResponseBean(){}

    public ResponseBean(String success, String msg, T data) {
        super();
        this.result = success;
        this.msg = msg;
        this.data = data;
    }

    public ResponseBean(T data, UnicomResponseEnums unicomResponseEnums){
        super();
        this.data = data;
        this.result = unicomResponseEnums.getCode();
        this.msg = unicomResponseEnums.getMsg();
    }
    @Override
    public String toString() {
        /*return "ResponseBean{" +
                "success=" + success +
                ", data=" + data +
                ", errCode='" + errCode + '\'' +
                ", errMsg='" + errMsg + '\'' +
                '}';*/
        return "ResponseBean{" +
                "result=" + result +
                ",message=" + msg +
                ", data=" + data +
                '}';
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
