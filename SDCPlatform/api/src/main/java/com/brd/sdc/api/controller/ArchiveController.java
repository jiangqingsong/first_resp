package com.brd.sdc.api.controller;

import com.brd.sdc.api.beans.Archive;
import com.brd.sdc.api.dao.ArchiveMapper;
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
 * @Description 定级备案controller入口
 * @createTime 2020年02月27日 13:32:00
 */
@RestController
@RequestMapping("/reference_manager")
public class ArchiveController {

    @Autowired
    ArchiveMapper archiveMapper;

    @PostMapping("/updateById")
    void updateArchiveBySysname(Archive archive) {
        archiveMapper.update(archive);
    }

    @PostMapping(value = {"/insertAsset"})
    void saveAsset(@RequestBody Archive archive) {
        archiveMapper.saveArchive(archive);
    }

    @GetMapping("/removeArchive")
    void removeArchive(@RequestParam String sysName){
        Archive archive = new Archive();
        archive.setSys_name(sysName);
        archiveMapper.removeArchive(archive);
    };
}
