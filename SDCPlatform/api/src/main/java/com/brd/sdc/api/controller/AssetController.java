package com.brd.sdc.api.controller;

import com.brd.sdc.api.beans.Asset;
import com.brd.sdc.api.dao.AssetMapper;
import com.brd.sdc.api.response.ResponseBean;
import com.brd.sdc.api.response.UnicomResponseEnums;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @version 1.0.0
 * @ClassName Asset.java
 * @Description 资产信息修改controller入口
 * @createTime 2020年02月27日 13:32:00
 */
@RestController
@RequestMapping("/asset_manager")
public class AssetController {

    @Autowired
    AssetMapper assetMapper;

    @PostMapping("/updateById")
    void updateAssetById(Asset asset) {
        assetMapper.update(asset);
    }

    @PostMapping(value = {"/insertAsset"})
    void saveAsset(@RequestBody Asset asset) {
        assetMapper.saveAsset(asset);
    }

    @PostMapping(value = {"/insertAssetList"})
    void saveAssetList(@RequestBody List<Asset> list){
        assetMapper.saveAssetList(list);
    }

    @GetMapping("/test")
    public ResponseBean<List<Asset>> testReturn(){
        Asset asset = new Asset();
        Asset asset1 = new Asset();
        asset.setAsset_id("1");
        asset.setAsset_name("test1");
        asset.setAsset_type("t1");
        asset.setDate("20200202");
        asset1.setAsset_id("1");
        asset1.setAsset_name("test1");
        asset1.setAsset_type("t1");
        asset1.setDate("20200202");
        List<Object> list = new ArrayList<>();
        list.add(asset);
        list.add(asset1);

        ResponseBean<List<Asset>> responseBean = new ResponseBean(list, UnicomResponseEnums.SUCCESS);
        return responseBean;
    }

}
