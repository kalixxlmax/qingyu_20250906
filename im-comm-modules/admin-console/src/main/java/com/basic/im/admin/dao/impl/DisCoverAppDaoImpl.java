package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.DisCoverAppDao;
import com.basic.im.admin.entity.DisCoverApp;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Repository
public class DisCoverAppDaoImpl extends MongoRepository<DisCoverApp, ObjectId> implements DisCoverAppDao {

    @Override
    public Class<DisCoverApp> getEntityClass() {
        return DisCoverApp.class;
    }


    @Override
    public PageResult<DisCoverApp> getDisCoverApp(int page, int limit, String keyword) {
        PageResult<DisCoverApp> result=new PageResult<>();
        Query query=createQuery();
        if(!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("name").regex(keyword));
        }
        result.setCount(count(query));
        query.with(createPageRequest(page-1,limit));
        ascByquery(query,"sequence");
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public void deleteDisCoverApp(ObjectId id) {
        deleteById(id);
    }

    @Override
    public void updateDisCoverAppIsShow(byte isShow, ObjectId id) {
        Query query = createQuery(id);
        Update update = new Update();
        update.set("isShow",isShow);
        update(query,update);
    }

    @Override
    public void updateDisCoverApp(DisCoverApp disCoverApp) {
        Query query = createQuery(disCoverApp.getId());
        Update update = new Update();
        update.set("name",disCoverApp.getName());
        update.set("sequence",disCoverApp.getSequence());
        update.set("imgUrl",disCoverApp.getImgUrl());
        update.set("linkAddres",disCoverApp.getLinkAddres());
        update.set("isShow",disCoverApp.getIsShow());
        update.set("modirTime", DateUtil.currentTimeSeconds());
        update(query,update);
    }

    @Override
    public List<DisCoverApp> findDisCoverBySequence(int sequence) {
        Query query = createQuery("sequence",sequence);
        return queryListsByQuery(query);
    }


    @Override
    public DisCoverApp addDisCoverApp(DisCoverApp disCoverApp) {
        disCoverApp.setCreateTime(DateUtil.currentTimeSeconds());
        disCoverApp.setModirTime(DateUtil.currentTimeSeconds());
        DisCoverApp save = save(disCoverApp);
        return save ;
    }


}
