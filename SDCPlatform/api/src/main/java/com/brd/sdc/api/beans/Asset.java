package com.brd.sdc.api.beans;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName Asset.java
 * @Description TODO
 * @createTime 2020年02月27日 13:32:00
 */
public class Asset {
    private String asset_id;
    private String asset_name;
    private String asset_type ;
    private String date;

    public String getAsset_id() {
        return asset_id;
    }

    public void setAsset_id(String asset_id) {
        this.asset_id = asset_id;
    }

    public String getAsset_name() {
        return asset_name;
    }

    public void setAsset_name(String asset_name) {
        this.asset_name = asset_name;
    }

    public String getAsset_type() {
        return asset_type;
    }

    public void setAsset_type(String asset_type) {
        this.asset_type = asset_type;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
