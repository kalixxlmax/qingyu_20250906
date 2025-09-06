package com.basic.im.admin.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.DisCoverAppDao;
import com.basic.im.admin.entity.DisCoverApp;
import com.basic.im.admin.service.DisCoverAppManager;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Service
public class DisCoverAppManagerImpl implements DisCoverAppManager {

    @Autowired
    private DisCoverAppDao disCoverAppDao;

    @Override
    public PageResult<DisCoverApp> getDisCoverApp(int page, int limit, String keyword) {
        return disCoverAppDao.getDisCoverApp(page, limit, keyword);
    }

    @Override
    public void deleteDisCoverApp(ObjectId id) {
        disCoverAppDao.deleteDisCoverApp(id);
    }

    @Override
    public void updateDisCoverAppIsShow(byte isShow, ObjectId id) {
        disCoverAppDao.updateDisCoverAppIsShow(isShow,id);
    }

    @Override
    public void updateDisCoverApp(DisCoverApp disCoverApp) {
        disCoverAppDao.updateDisCoverApp(disCoverApp);
    }


    @Override
    public DisCoverApp addDisCoverApp(DisCoverApp disCoverApp) {
        DisCoverApp data = disCoverAppDao.addDisCoverApp(disCoverApp);
        return data;
    }
}
