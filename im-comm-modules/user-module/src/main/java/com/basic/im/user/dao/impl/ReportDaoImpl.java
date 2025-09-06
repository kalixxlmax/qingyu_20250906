package com.basic.im.user.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.ReportDao;
import com.basic.im.user.entity.Report;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Repository
public class ReportDaoImpl extends MongoRepository<Report,ObjectId> implements ReportDao {

    @Override
    public Class<Report> getEntityClass() {
        return Report.class;
    }

    @Override
    public void addReport(Integer userId, Integer toUserId, int reason, String roomId, String webUrl,int reportType,String reportInfo) {
        Report report=new Report();
        if(null != userId) {
            report.setUserId(userId);
        }
        if(null != toUserId) {
            report.setToUserId(toUserId);
        }
        report.setReason(reason);
        if(!StringUtil.isEmpty(roomId)) {
            report.setRoomId(roomId);
        }
        if(!StringUtil.isEmpty(webUrl)){
            report.setWebPageUrl(webUrl);
            try {
                URL requestUrl = new URL(webUrl);
                report.setWebUrl(requestUrl.getHost());
            } catch (MalformedURLException e) {
                report.setWebUrl(webUrl);
            }
            report.setWebStatus(1);
        }
        if(0 != reportType){
            report.setReportType(reportType);
        }
        if(!StringUtil.isEmpty(reportInfo)){
            report.setReportInfo(reportInfo);
        }
        report.setTime(DateUtil.currentTimeSeconds());
        getDatastore().save(report);
    }

    @Override
    public Report getReport(ObjectId id) {
        return get(id);
    }

    @Override
    public List<Report> getReportListByWebUrl(String webUrl) {
        Query query=createQuery();
        query.addCriteria(contains("webUrl",webUrl));
        return queryListsByQuery(query);
    }

    @Override
    public List<Report> getReportList(long userId, String receiver, int pageIndex, int pageSize, int type) {
        Query query =createQuery();
        if(type == 0){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)){
                addToQuery(query,"toUserId",userId);
            }else{
               query.addCriteria(Criteria.where("toUserId").ne(0));
            }
           addToQuery(query,"roomId",null);
        }else if(type == 1){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)) {
                addToQuery(query,"roomId",receiver);
            }
            query.addCriteria(Criteria.where("roomId").ne(null));
        }else if(type == 2){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)) {
                addToQuery(query,"webUrl",receiver);
            }

            query.addCriteria(Criteria.where("webUrl").ne(null));
            query.addCriteria(Criteria.where("toUserId").is(0));
        }

        descByquery(query,"time");
        return  queryListsByQuery(query,pageIndex,pageSize);
    }

    @Override
    public PageResult<Report>  getReportListResult(long userId, String receiver, int pageIndex, int pageSize, int type) {
        Query query =createQuery();
        if(type == 0){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)){
                addToQuery(query,"toUserId",Integer.valueOf(receiver));
            }else{
                query.addCriteria(Criteria.where("toUserId").ne(0));
            }
            addToQuery(query,"roomId",null);
        }else if(type == 1){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)){
                addToQuery(query,"roomId",receiver);
            }else{
                query.addCriteria(Criteria.where("roomId").ne(null));
            }
            query.addCriteria(new Criteria().orOperator(Criteria.where("roomId").ne("")));

        }else if(type == 2){
            if(0!=userId) {
                addToQuery(query,"userId",userId);
            }
            if(!StringUtil.isEmpty(receiver)){
                addToQuery(query,"webUrl",receiver);
            }else{
                query.addCriteria(Criteria.where("webUrl").ne(null));
            }
            query.addCriteria(Criteria.where("toUserId").is(0));
        }

        descByquery(query,"time");
        PageResult<Report> pageResult=new PageResult<>();
        pageResult.setCount(count(query));
        pageResult.setData(queryListsByQuery(query,pageIndex,pageSize));
        return pageResult;
    }

    @Override
    public void deleteReportById(ObjectId id) {
       deleteById(id);
    }

    @Override
    public void deleteReport(Integer userId, String roomId) {
        Query query = createQuery();
        if(null != userId){
            Criteria criteria = createCriteria().orOperator(createCriteria("userId", userId), createCriteria("toUserId", userId));
            query.addCriteria(criteria);
        }
        else if (null != roomId) {
            addToQuery(query,"roomId",new ObjectId(roomId));
        }
        deleteByQuery(query);
    }
}
