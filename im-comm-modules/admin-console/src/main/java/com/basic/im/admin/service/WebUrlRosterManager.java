package com.basic.im.admin.service;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.WebUrlRoster;
import org.bson.types.ObjectId;

public interface WebUrlRosterManager {

    void addWebUrlRosterRecord(String webUrl, byte urlType);

    byte checkWebUrlType(String webUrl);

    PageResult<WebUrlRoster> getWebUrlRosterList(String webUrl,  byte webUrlType, int page,  int limit);

    void  deleteWebUrlRoster(ObjectId webUrlRostreId);

    void updateWebUrlRosterStatus(ObjectId webUrlRostreId,byte webUrlType);

    void addWebUrlRoster(String webUrl, byte webUrlType);
}
