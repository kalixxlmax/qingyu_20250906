package com.basic.im.company.dao.impl;

import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.company.CompanyConstants;
import com.basic.im.company.dao.EmployeeDao;
import com.basic.im.company.entity.Employee;
import com.basic.im.i18n.LocaleMessageUtils;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class EmployeeDaoImpl extends BaseMongoRepository<Employee,ObjectId> implements EmployeeDao {



    @Override
    public Class<Employee> getEntityClass() {
        return Employee.class;
    }

    //添加员工（单个）
    @Override
    public ObjectId addEmployee(Employee employee) {
        employee.setId(new ObjectId());
        getDatastore().save(employee);
        return employee.getId();
    }

    //添加员工（多个）
    @Override
    public List<Employee> addEmployees(List<Integer> userId, ObjectId companyId, ObjectId departmentId) {
        Locale requestLocale = LocaleMessageUtils.getRequestLocale();
        for(Iterator<Integer> iter = userId.iterator(); iter.hasNext();){
            Employee employee = new Employee(departmentId, companyId ,iter.next());
            //emp.setPosition(LocaleMessageUtils.getMessage("staff",requestLocale));
            getDatastore().save(employee);//存入员工数据
        }
        //将整个部门的员工数据封装返回
        Query query = createQuery("departmentId",departmentId);

        return queryListsByQuery(query);
    }


    //修改员工信息
    @Override
    public Employee modifyEmployees(Employee employee) {
        int userId = employee.getUserId();
        ObjectId companyId = employee.getCompanyId();

        if(userId == 0 || companyId == null){
            return null;
        }
        Query query = createQuery("userId",userId);
        addToQuery(query,"companyId",companyId);
        Update ops = createUpdate();
        if(null != employee.getDepartmentId()) {
            ops.set("departmentId", employee.getDepartmentId());
        }
        if(0 <= employee.getRole() && employee.getRole() <= 3) {
            ops.set("role", employee.getRole());
        }
        if(!StringUtil.isEmpty(employee.getPosition())) {
            ops.set("position", employee.getPosition());
        }
       
        return getDatastore().findAndModify(query, ops,new FindAndModifyOptions().returnNew(true), getEntityClass());
    }


    //通过userId查找员工
    @Override
    public List<Employee> findByUserId(int userId){
        Query query = createQuery("userId",userId);
        return queryListsByQuery(query);
    }


    //查找公司中某个角色的所有员工
    @Override
    public List<Employee> findByRole(ObjectId companyId, int role) {
        Query query =createQuery("companyId",companyId);
        addToQuery(query,"role",role);
        return queryListsByQuery(query);
    }


    //删除整个部门的员工
    @Override
    public void delEmpByDeptId(ObjectId departmentId) {
        //根据部门id找到员工
        Query query =createQuery("departmentId",departmentId);
        deleteByQuery(query);
    }

    // 删除整个公司员工
    @Override
    public void deleteAllEmployee(ObjectId companyId) {
        Query query = createQuery("companyId",companyId);
        deleteByQuery(query);
    }

    //删除员工
    @Override
    public void deleteEmployee(List<Integer> userIds, ObjectId departmentId) {
        Query query =createQuery("departmentId",departmentId);
        query.addCriteria(Criteria.where("userId").in(userIds));
        deleteByQuery(query);
    }

    @Override
    public void delEmpByUserId(Integer userId) {
        Query query = createQuery("userId",userId);
       deleteByQuery(query);
    }

    //根据公司ID查询员工(员工列表)
    @Override
    public List<Employee> compEmployeeList(ObjectId companyId) {
        Query query = createQuery("companyId",companyId);
        return queryListsByQuery(query);
    }


    @Override
    public boolean departIsExistsEmployeeData(ObjectId departmentId){
        Query query =createQuery("departmentId",departmentId);
        return exists(query);
    }


    @Override
    public boolean employeeIsExists(ObjectId companyId, int userId) {
        Query query = createQuery("companyId",companyId);
        query.addCriteria(Criteria.where("userId").is(userId));
        return exists(query);
    }

    //根据部门ID查询员工(部门员工列表)
    @Override
    public List<Employee> departEmployeeList(ObjectId departmentId) {
        Query query =createQuery("departmentId",departmentId);
        return queryListsByQuery(query);
    }


    //根据id查找员工
    @Override
    public Employee findById(ObjectId employeeId) {
       return get(employeeId);
    }




    //查找某个员工的角色，通过公司id
    @Override
    public byte findRole(ObjectId companyId, int userId) {
       /* Query query = createQuery("companyId",companyId);
        addToQuery(query,"userId", userId);*/
        Document query=new Document("companyId",companyId).append("userId", userId);
        Object role = queryOneField("role",query);
        if(role!=null && NumberUtil.isNumeric(role.toString())) {
            return ((Integer)role).byteValue();
        }
        return -1;
    }


    //查找某个员工的角色
    @Override
    public byte findRoleByDepartmentId(ObjectId companyId, int userId) {

        if(companyId!=null) {
            return findRole(companyId,userId);
        }
        return -1;
    }

    //删除员工(根据公司id）
    @Override
    public void delEmpByCompId(ObjectId companyId, int userId) {
        Query query = createQuery("companyId",companyId);
        addToQuery(query,"userId", userId);
       deleteByQuery(query);
    }

    /**
     * 根据用户id来修改员工信息
     */
    @Override
    public Employee modifyEmployeesByuserId(int userId) {
        Employee employeeInfo = new Employee();
        if (!StringUtil.isEmpty(String.valueOf(userId))) {
            // 根据用户id来查询员工信息
            Query query =createQuery("userId", userId);
            // 修改

            // 赋值
            List<Employee> employeeList =queryListsByQuery(query);
            for (Employee employee2 : employeeList) {
                if (null != employee2 && !"".equals(employee2)) {
                    Update uo =createUpdate();
                   
                    // employeeInfo = getDatastore().findAndModify(query,uo);
                    update(createQuery("_id",employee2.getId()),uo);
                }

            }
        }
        return employeeInfo;
    }

  

    /**
     *根据公司，部门，用户id来查询出员工信息
     */
    @Override
    public Employee findEmployee(Employee employee) {
        Query query =createQuery("userId", employee.getUserId());
		if(null!=employee.getDepartmentId()){
            addToQuery(query,"departmentId",employee.getDepartmentId());
		}
		if(null!=employee.getCompanyId()){
            addToQuery(query,"companyId",employee.getCompanyId());
		}
        if(null!=employee.getCompanyId()){
            addToQuery(query,"companyId",employee.getCompanyId());
        }

        return findOne(query);
    }
    @Override
    public Employee getEmployeeByUserId(ObjectId companyId, int userId) {
        Query query =createQuery("companyId", companyId);
        addToQuery(query,"userId",userId);
        return findOne(query);
    }

    
    /**
     * @author xie yuan yang
     * @date 2019/9/23 17:30
     *  查询员工列表  分页
     */
    @Override
    public List<Employee> compEmployeeList(ObjectId companyId, int pageSize, int pageIndex) {
        Query query =createQuery("companyId",companyId);
        List<Employee> employee = queryListsByQuery(query,pageIndex,pageSize);
       /* employee.forEach(emp->{
            User user = userCoreService.getUser(emp.getUserId());
            //emp.setNickname(user.getNickname());
        });*/
        return employee;
    }

    @Override
    public long calculateEmployeeNumber(ObjectId companyId) {
        Query query =createQuery("companyId",companyId);
        return count(query);
    }

    /**
     * @author xie yuan yang
     * @date 2019/9/24 18:44
     *      根据部门编号查询员工
     */
    public List<Employee> employeeList(ObjectId departmentId) {
        Query query =createQuery("departmentId",departmentId);
        return queryListsByQuery(query);
    }

    @Override
    public List<Integer> queryEmployeeUserIdList(ObjectId companyId){
        Query query =createQuery("companyId",companyId);
        return distinct("userId",query,Integer.class);

    }

    @Override
    public ObjectId queryEmployeeCompanyId(Integer userId){
        return (ObjectId) queryOneField(getEntityClass(),"companyId","userId",userId);
    }

}
