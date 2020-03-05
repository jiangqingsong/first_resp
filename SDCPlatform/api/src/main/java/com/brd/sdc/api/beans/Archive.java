package com.brd.sdc.api.beans;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName Archive.java
 * @Description 定级备案管理
 * @createTime 2020年03月02日 13:28:00
 */
public class Archive {
    private String sys_name;
    private String archive_level;
    private String created_time;
    private String department_name;
    private String archive_sysy_state;
    private String creater;
    private String create_time;
    private String latest_valuation_time;

    public String getSys_name() {
        return sys_name;
    }

    public void setSys_name(String sys_name) {
        this.sys_name = sys_name;
    }

    public String getArchive_level() {
        return archive_level;
    }

    public void setArchive_level(String archive_level) {
        this.archive_level = archive_level;
    }

    public String getCreated_time() {
        return created_time;
    }

    public void setCreated_time(String created_time) {
        this.created_time = created_time;
    }

    public String getDepartment_name() {
        return department_name;
    }

    public void setDepartment_name(String department_name) {
        this.department_name = department_name;
    }

    public String getArchive_sysy_state() {
        return archive_sysy_state;
    }

    public void setArchive_sysy_state(String archive_sysy_state) {
        this.archive_sysy_state = archive_sysy_state;
    }

    public String getCreater() {
        return creater;
    }

    public void setCreater(String creater) {
        this.creater = creater;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getLatest_valuation_time() {
        return latest_valuation_time;
    }

    public void setLatest_valuation_time(String latest_valuation_time) {
        this.latest_valuation_time = latest_valuation_time;
    }
}
