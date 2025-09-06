package com.basic.im.company.dao;

import com.basic.common.model.PageResult;
import com.basic.im.company.entity.Company;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

import java.util.List;

public interface CompanyDao extends IBaseMongoRepository<Company,ObjectId> {
    /**
     * 创建公司
     **/
    Company addCompany(String companyName, int createUserId, ObjectId rootDpartId);

    /**
     * 根据创建者Id查找公司
     **/
    Company findCompanyByCreaterUserId(int createUserId);

    List<Company> findCompanyListByCreateUserId(int createUserId);

    /**
     * 修改公司信息
     **/
    Company modifyCompany(Company company);

    /**
     * 通过公司名称的关键字模糊查找公司
     **/
    List<Company> findCompanyByName(String keyworld);

    /**
     * 根据公司id查找公司
     **/
    Company findById(ObjectId companyId);

    /**
     * 获得所有公司
     **/
    PageResult<Company> companyList(int pageSize, int pageIndex, String keyword);

    /**
     * 根据公司名称查找公司，精准查找
     **/
    Company findOneByName(String companyName);

    /**
     * 返回某个特定状态值的公司
     **/
    List<Company> findByType(int type);

    void deleteComplay(Company company);

    /**
     * 通过公司名称的关键字模糊查找公司
     **/
    PageResult<Company> findCompanyByNameLimit(String keyworld, int page, int limit);

    /**
     * 审核公司
     * @param companyId
     * @return
     */
    boolean checkedCompany(ObjectId companyId,byte isCheck);

    /**
     * 新建公司
     */
    Company createCompany(Company company);
}
