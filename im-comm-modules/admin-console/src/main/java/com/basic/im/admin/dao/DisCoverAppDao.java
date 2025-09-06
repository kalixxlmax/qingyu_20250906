package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.DisCoverApp;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
public interface DisCoverAppDao extends IBaseMongoRepository<DisCoverApp, ObjectId> {

    //分页查询发现页
    PageResult<DisCoverApp> getDisCoverApp(int page, int limit , String keyword);

    //删除发现页
    void deleteDisCoverApp(ObjectId id);

    //修改发现页状态
    void updateDisCoverAppIsShow(byte isShow,ObjectId id);

    //修改发现页
    void updateDisCoverApp(DisCoverApp disCoverApp);

    List<DisCoverApp> findDisCoverBySequence(int sequence);

    DisCoverApp addDisCoverApp(DisCoverApp disCoverApp);
}
