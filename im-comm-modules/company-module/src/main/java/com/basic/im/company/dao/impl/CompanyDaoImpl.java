package com.basic.im.company.dao.impl;

import com.mongodb.client.result.UpdateResult;
import com.basic.common.model.PageResult;
import com.basic.im.company.dao.CompanyDao;
import com.basic.im.company.entity.Company;
import com.basic.im.utils.SKBeanUtils;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Repository
public class CompanyDaoImpl extends BaseMongoRepository<Company,ObjectId> implements CompanyDao {

    @Override
    public Class<Company> getEntityClass() {
        return Company.class;
    }


    //创建公司
    @Override
    public Company addCompany(String companyName, int createUserId, ObjectId rootDpartId) {

        Company company = new Company();
        List<ObjectId> list = new ArrayList<ObjectId>();
        list.add(rootDpartId);

        company.setCompanyName(companyName);
        company.setCreateUserId(createUserId);
        company.setDeleteUserId(0);
        company.setCreateTime(DateUtil.currentTimeSeconds());
        company.setRootDpartId(list);
        company.setNoticeContent("");
        company.setDeleteTime(0);
        company.setNoticeTime(0);
        company.setEmpNum(1);

        //创建公司是否需要审核
        company.setIsChecked( (1==SKBeanUtils.getSystemConfig().getCreateCompamyIsNeedCheck()) ? 0 : (byte) 1 );

        //存入公司数据
        ObjectId companyId = (ObjectId) getDatastore().save(company).getId();
        company.setId(companyId);

        return company;
    }


    //根据创建者Id查找公司
    @Override
    public Company findCompanyByCreaterUserId(int createUserId) {
        //根据创建者Id查找公司，同时排除掉deleteUserId != 0 的数据(deleteUserId != 0 :表示已经删除）
        Query query =createQuery("createUserId",createUserId);
        addToQuery(query,"deleteUserId",0);
        return findOne(query);

    }

    @Override
    public List<Company> findCompanyListByCreateUserId(int createUserId) {
        Query query =createQuery("createUserId",createUserId);
        addToQuery(query,"deleteUserId",0);
        return queryListsByQuery(query);
    }

    //修改公司信息
    @Override
    public Company modifyCompany(Company company) {
        ObjectId companyId = company.getId();
        if(companyId == null){
            return null;
        }

        Query query = createQuery("_id",companyId);
        Update ops = createUpdate();
        if(!StringUtil.isEmpty(company.getCompanyName())) {
            ops.set("companyName", company.getCompanyName());
        }
        if(0 != company.getCreateUserId()) {
            ops.set("createUserId", company.getCreateUserId());
        }
        if(0 != company.getDeleteUserId()) {
            ops.set("deleteUserId", company.getDeleteUserId());
        }

        if(!StringUtil.isEmpty(company.getNoticeContent())){
            ops.set("noticeContent", company.getNoticeContent());
            ops.set("noticeTime", DateUtil.currentTimeSeconds());
        }
        if(0 != company.getDeleteTime()) {
            ops.set("deleteTime", company.getDeleteTime());
        }
        if(0 != company.getEmpNum()) {
            ops.set("empNum", company.getEmpNum());
        }

        if(!ops.getUpdateObject().isEmpty()){
            //更新数据
            update(query,ops);
        }

        return findById(companyId);
    }


    //通过公司名称的关键字模糊查找公司
    @Override
    public List<Company> findCompanyByName(String keyworld) {

        Query query = createQuery();

        //排除没有通过审核的公司
        query.addCriteria(Criteria.where("isChecked").is(1));
        //除去执行过删除操作,被隐藏的公司
        query.addCriteria(Criteria.where("deleteUserId").is(0));

        query.addCriteria(Criteria.where("companyName").regex(keyworld));

        //忽略大小写进行模糊匹配
        List<Company> companys = queryListsByQuery(query);

        return companys;
    }

    //根据公司id查找公司
    @Override
    public Company findById(ObjectId companyId){
       return get(companyId);
    }



    //获得所有公司
    @Override
    public PageResult<Company> companyList(int pageSize, int pageIndex,String keyword) {
        PageResult<Company> list = new PageResult<>();
        //查找没有被隐藏起来的公司
        Query query =createQuery("deleteUserId",0);
        if(!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("companyName").regex(keyword));
        }
        descByquery(query,"createTime");
        list.setCount(count(query));
        list.setData(queryListsByQuery(query, pageIndex - 1, pageSize));
        return list;
    }


    //根据公司名称查找公司，精准查找
    @Override
    public Company findOneByName(String companyName) {
        //查找公司名称完全匹配，且没有被隐藏起来的公司
        Query query =createQuery("deleteUserId",0);
        addToQuery(query,"companyName",companyName);
        return findOne(query);
    }


    //返回某个特定状态值的公司
    @Override
    public List<Company> findByType(int type) {
        Query query =createQuery("type",type);
       return queryListsByQuery(query);
    }

    @Override
    public void deleteComplay(Company company) {

        deleteById(company.getId());
    }

    /**
     * @author xie yuan yang
     * @date 2019/11/21 11:03
     *      通过公司名称的关键字模糊查找公司
     */
    @Override
    public PageResult<Company> findCompanyByNameLimit(String keyworld, int page, int limit) {
        PageResult<Company> data = new PageResult<>();
        if(StringUtil.isEscapeChar(keyworld)){
            // 特殊字符开头 .*+ 会导致异常，这里做一下特殊处理
            keyworld = keyworld.replaceAll("^[+*.](.*?)","");
            if(StringUtil.isEmpty(keyworld)){
                return data;
            }
        }
        Query query = createQuery();
        query.addCriteria(Criteria.where("companyName").regex(keyworld));

        //忽略大小写进行模糊匹配
        List<Company> companys = queryListsByQuery(query);

        //除去执行过删除操作,被隐藏的公司
        for (Iterator<Company> iter = companys.iterator(); iter.hasNext();) {
            Company company = iter.next();
            if (company.getDeleteUserId() != 0) {   //将DeleteUserId不为0的数据剔除
                iter.remove();
            }
        }

        data.setData(companys);
        data.setCount(count(query));
        return data;
    }


    @Override
    public boolean checkedCompany(ObjectId companyId,byte isCheck) {
        Query query = createQuery("_id",companyId);
        Update ops = createUpdate();
        ops.set("isChecked", isCheck);
        return update(query,ops).getModifiedCount() > 0;
    }

    @Override
    public Company createCompany(Company company){
        return save(company);
    }
}
