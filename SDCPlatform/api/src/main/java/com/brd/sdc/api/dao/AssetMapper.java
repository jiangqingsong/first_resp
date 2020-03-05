package com.brd.sdc.api.dao;

import com.brd.sdc.api.beans.Asset;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName AssetMapper.java
 * @Description TODO
 * @createTime 2020年02月28日 14:10:00
 */
@Mapper
public interface AssetMapper {
    @Insert("insert into sdc_asset_update(asset_id, asset_name, asset_type , date ) " +
            "values (#{asset_id}, #{asset_name}, #{asset_type}, #{date})")
    public void saveAsset(Asset asset);

    @Update("update sdc_asset_update set asset_id=#{asset_id}," +
            "asset_name=#{asset_name},asset_type=#{asset_type},date=#{date}" +
            "where asset_id=#{asset_id}")
    public void update(Asset asset);

    @Insert({
            "<script>",
            "insert into sdc_asset_update(asset_id, asset_name, asset_type, date) values ",
            "<foreach collection='list' item='item' index='index' separator=','>",
            "(#{item.asset_id}, #{item.asset_name}, #{item.asset_type}, #{item.date})",
            "</foreach>",
            "</script>"
    })
    void saveAssetList(List<Asset> list);
}
