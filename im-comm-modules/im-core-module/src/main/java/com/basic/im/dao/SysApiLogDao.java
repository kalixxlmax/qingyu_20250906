package com.basic.im.dao;

import com.basic.common.model.PageResult;
import com.basic.im.entity.SysApiLog;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface SysApiLogDao extends IMongoDAO<SysApiLog, ObjectId> {

    PageResult<SysApiLog> getSysApiLog(String keyword, int pageIndex, int pageSize);

    void deleteSysApiLog(ObjectId id);

    void deleteSysApiLogByTime(long time);
}
