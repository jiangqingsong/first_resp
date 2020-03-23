package com.brd.sdc.api.response;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName ResponseBean.java
 * @Description 返回的JSON数据结构标准
 * @createTime 2020年02月28日 17:26:00
 */
public class ResponseBean<T> {
    private String resultCode;
    private String resultMsg;
    private T data;

    public ResponseBean(){}

    public ResponseBean(String success, String msg, T data) {
        super();
        this.resultCode = success;
        this.resultMsg = msg;
        this.data = data;
    }

    public ResponseBean(T data, UnicomResponseEnums unicomResponseEnums){
        super();
        this.data = data;
        this.resultCode = unicomResponseEnums.getCode();
        this.resultMsg = unicomResponseEnums.getMsg();
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
                "result=" + resultCode +
                ",message=" + resultMsg +
                ", data=" + data +
                '}';
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
