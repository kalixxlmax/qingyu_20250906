package com.basic.im.manual.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.entity.Withdraw;
import com.basic.im.repository.MongoRepository;
import com.basic.im.manual.dao.WithdrawDao;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 17:10
 */
@Repository
public class WithdrawDaoImpl extends MongoRepository<Withdraw, ObjectId> implements WithdrawDao {
    @Override
    public MongoTemplate getDatastore() {
        return super.getDatastore();
    }

    @Override
    public Class<Withdraw> getEntityClass() {
        return Withdraw.class;
    }

    @Override
    public void addWithdraw(Withdraw entity) {
        getDatastore().save(entity);
    }

    @Override
    public Withdraw getWithdraw(ObjectId id) {
        return get(id);
    }

    @Override
    public PageResult<Withdraw> getWithdrawList(int pageIndex, int pageSize, String keyword, long startTime, long endTime) {
        PageResult<Withdraw> result = new PageResult<>();
        Query query = createQuery();

        query.with(createPageRequest(pageIndex,pageSize));
        if(!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("userId").is(Integer.valueOf(keyword)));
        }
        //时间范围查询
        if(0<startTime||0<endTime){
            query.addCriteria(Criteria.where("createTime").gt(startTime).lte(endTime));
        }

        descByquery(query,"createTime");
        result.setCount(count(query));
        result.setData(getDatastore().find(query,getEntityClass()));
        return result;
    }

    @Override
    public Map<String, Object> queryWithdraw(int pageIndex,int pageSize,String keyWord,long startTime,long endTime) {
        Map<String,Object> map = new HashMap<>(6);

       /* final MongoCollection<Document> collection =getDatastore().getCollection("withdraw");
        List<Document> pipeline=new ArrayList<>();
        Document group=new Document("$group", new BasicDBObject("_id", "$status")
                .append("sum",new Document("$sum","$money")));
        pipeline.add(group);*/


        Criteria criteria = createCriteria();

        criteria.and("status").ne(0);
        if(!StringUtil.isEmpty(keyWord)&& NumberUtil.isNum(keyWord)){
            criteria.and("userId").is(Integer.valueOf(keyWord));
        }
        Criteria timeCriteria = Criteria.where("createTime");
        if(0<startTime){
            timeCriteria.gt(startTime);
        }
        if(0<endTime){
            timeCriteria.lt(endTime);
        }
        if(0<startTime||0<endTime){
            criteria.andOperator(timeCriteria);
        }

        GroupOperation groupOperation = Aggregation.group("status").sum("money").as("total");

        Aggregation aggregation = Aggregation.newAggregation(Aggregation.match(criteria),
                Aggregation.sort(Sort.Direction.DESC,"createTime"),
                Aggregation.skip(pageIndex*pageSize),
                Aggregation.limit(pageSize),
                groupOperation,
                Aggregation.project("status","total")

        );


        AggregationResults<Document> aggregate = getDatastore().aggregate(aggregation, Withdraw.class, Document.class);
        Iterator<Document> iterator = aggregate.iterator();
        Document dbObject=null;
        // 总充值申请 充值成功 充值失败 申请中
        double totalRecharge = 0, successRecharge = 0, failureRecharge = 0, applyRecharge = 0,ignoreCount=0;
        while (iterator.hasNext()){
            dbObject = iterator.next();
            if(dbObject.get("_id").equals(1)){
                applyRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(2)){
                successRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(-1)){
                failureRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(-2)){
                ignoreCount = (double)dbObject.get("total");
            }
            totalRecharge = applyRecharge+successRecharge+failureRecharge;
        }




        map.put("totalWithdraw",totalRecharge);
        map.put("successWithdraw",successRecharge);
        map.put("failureWithdraw",failureRecharge);
        map.put("applyWithdraw",applyRecharge);
        map.put("ignoreCount",ignoreCount);
        return map;
    }

    @Override
    public Withdraw updateWithdraw(ObjectId id, Map<String, Object> map) {
        Query query = createQuery("_id",id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
//        getDatastore().updateFirst(query,ops,getEntityClass());
        return getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public void deleteWithdraw(ObjectId id) {
        deleteById(id);
    }
}
