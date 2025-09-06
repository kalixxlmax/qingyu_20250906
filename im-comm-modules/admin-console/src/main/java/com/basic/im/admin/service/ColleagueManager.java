package com.basic.im.admin.service;

import com.basic.common.model.PageResult;
import com.basic.im.company.entity.Company;
import com.basic.im.company.entity.Department;
import com.basic.im.company.entity.Employee;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @author xie yuan yang
 * @Date Created in 2019/11/22 14:12
 * @description TODO (web后台我的同事逻辑层)
 * @modified By:
 */
public interface ColleagueManager {

    /**
     * 公司列表
     */
    PageResult<Company> companyList(int pageSize, int pageIndex,String keyword);

    /**
     * 公司部门列表
     */
     List<Department> departmentList(String companyId);

    /**
     * 查询公司全部部门
     */
     Employee findEmployeeMsg(ObjectId id);

    /**
     *  查询公司全部部门
     */
     List<Department> departmentAllList(ObjectId companyId);

    /**
     * 根据公司编号 查询公司部门id
     */
     Department queryDepartmentId(ObjectId companyId);

    /**
     * 修改部门
     */
     Department webModifyDepartmentInfo(Department department,String oldDepartmentName);

    /**
     * 关键字查询公司
     */
     PageResult<Company> findCompanyByKeyworldLimit(String keyworld, int page, int limit);

    /**
     *  添加员工(支持多个)
     */
     ObjectId webAddEmployee(ObjectId companyId, ObjectId departmentId,int  userId,byte role);

    /**
     * 删除部门
     */
     void deleteDepartment(ObjectId departmentId);

    /**
     * 创建部门
     */
     Department createDepartment(ObjectId companyId, ObjectId parentId, String departName, int createUserId);

     Employee modifyEmpInfo(Employee employee);

    /**
     * 获取员工详情
     */
     Employee getEmployee(ObjectId employeeId);

    /**
     * 删除员工
     */
     void deleteEmployee(List<Integer> userIds, ObjectId departmentId);

    /**
     * 查询上级个数
     */
     int getRetractNumber(ObjectId id);

    /**
     *  创建公司
     */
     Company createCompany(String companyName, int createUserId);

    /**
     * 部门列表
     */
     List<Department> departmentList(ObjectId companyId);

    /**
     * 删除公司(即隐藏公司,不真正删除)
     */
     void deleteCompany(ObjectId companyId, int userId);


    void checkedCompany(ObjectId companyId,byte isCheck);

    /**
     * 新建公司
     */
    Company createCompany(Company company);
}
