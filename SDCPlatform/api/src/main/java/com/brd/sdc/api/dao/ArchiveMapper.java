package com.brd.sdc.api.dao;

import com.brd.sdc.api.beans.Archive;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName AssetMapper.java
 * @Description 定级备案crud
 * @createTime 2020年02月28日 14:10:00
 */
@Mapper
public interface ArchiveMapper {
    @Insert("insert into archive(sys_name, archive_level, created_time , department_name," +
            " archive_sysy_state,creater,create_time,latest_valuation_time) " +
            "values (#{sys_name},#{archive_level},#{created_time},#{department_name}," +
            "#{archive_sysy_state},#{creater},#{create_time},#{latest_valuation_time})")
    public void saveArchive(Archive archive);

    @Update("update archive set " +
            "sys_name=#{sys_name}," +
            "archive_level=#{archive_level}," +
            "created_time=#{created_time}," +
            "department_name=#{department_name}" +
            "archive_sysy_state=#{archive_sysy_state}" +
            "creater=#{creater}" +
            "latest_valuation_time=#{latest_valuation_time}" +
            "date=#{date}" +
            "where sys_name=#{sys_name}")
    public void update(Archive asset);

    @Delete("delete from archive where sys_name=#{asset_name}")
    void removeArchive(Archive asset);
}
