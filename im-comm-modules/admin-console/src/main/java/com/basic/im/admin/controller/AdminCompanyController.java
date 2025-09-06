package com.basic.im.admin.controller;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.ResultDepartment;
import com.basic.im.admin.service.impl.ColleagueManagerImpl;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.company.CompanyConstants;
import com.basic.im.company.dao.CompanyDao;
import com.basic.im.company.dao.DepartmentDao;
import com.basic.im.company.dao.EmployeeDao;
import com.basic.im.company.entity.Company;
import com.basic.im.company.entity.Department;
import com.basic.im.company.entity.Employee;
import com.basic.im.company.service.impl.CompanyManagerImpl;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.vo.JSONMessage;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xie yuan yang
 * @description 我的同事操作
 * @Date Created in 2019/11/21 17:38
 */
@ApiIgnore
@RestController
@RequestMapping("/console")
public class AdminCompanyController {

    @Autowired
    private ColleagueManagerImpl colleagueManager;

    @Autowired
    private UserCoreService userCoreService;

    @Autowired
    private CompanyManagerImpl companyManager;

    @Autowired
    private CompanyDao companyDao;

    @Autowired
    private EmployeeDao employeeDao;

    @Autowired
    private DepartmentDao departmentDao;


    /**
     *  公司列表
     **/
    @RequestMapping("/web/company/list")
    public JSONMessage companyList (@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "15") int limit,
                                    @RequestParam(defaultValue = "") String keyword){
        PageResult<Company> data = colleagueManager.companyList(limit,page,keyword);
        return JSONMessage.success(data);
    }



    /**
     * 部门列表
     */
    @RequestMapping("/find/department/list")
    @ApiImplicitParam(paramType="query" , name="companyId" , value="公司编号",dataType="String",required=true)
    public JSONMessage findDepartmentList (@RequestParam String companyId){
        Department rootDepartment = departmentDao.queryRootDepartment(new ObjectId(companyId));
        List<Department> departments = departmentDao.findChildDepartmeny(rootDepartment.getId());
        List<ResultDepartment> resultDepartments = new ArrayList<>();
        //一级节点
        departments.forEach(department -> { resultDepartments.add(new ResultDepartment(department.getId().toString(),department.getDepartName())); });
        //二级节点
        for (ResultDepartment resultDepartment : resultDepartments) {
            //子部门
            List<Department> departments_ = departmentDao.findChildDepartmeny(new ObjectId(resultDepartment.getId()));
            //员工
            List<Employee> employees_ = employeeDao.departEmployeeList(new ObjectId(resultDepartment.getId()));

            List<ResultDepartment> resultDepartments_ = new ArrayList<>();
            for (Department department : departments_) {
                resultDepartments_.add(new ResultDepartment(department.getId().toString(), department.getDepartName(),recursionDepartment(department)));
            }
            for (Employee employee : employees_) {
                User user = userCoreService.getUser(employee.getUserId());
                resultDepartments_.add(new ResultDepartment(employee.getId().toString(),user.getNickname(), employee.getRole() , employee.getPosition() , user.getSettings().getOpenService(),employee.getUserId(), employee.getDepartmentId().toString()));
            }
            resultDepartment.setChildren(resultDepartments_);
        }
        return JSONMessage.success(resultDepartments);
    }

    /**
     * 多级部门
     */
    private List<ResultDepartment> recursionDepartment(Department department){
        List<ResultDepartment> departments = new ArrayList<>();
        List<Department> departments_ = departmentDao.findChildDepartmeny(department.getId());
        List<Employee> employees_ = employeeDao.departEmployeeList(department.getId());

        departments_.stream().forEach(depar -> {
            List<ResultDepartment> resultDepartments = recursionDepartment(depar);
            departments.add(new ResultDepartment(depar.getId().toString(), depar.getDepartName(),resultDepartments));
        });

        employees_.stream().forEach(employee -> {
            User user = userCoreService.getUser(employee.getUserId());
            departments.add(new ResultDepartment(employee.getId().toString(),user.getNickname(), employee.getRole() , employee.getPosition() , user.getSettings().getOpenService(),employee.getUserId(), employee.getDepartmentId().toString()));
        });
        return departments;
    }


    /**
     * 删除员工
     */
    @ApiOperation("删除员工")
    @RequestMapping("/web/employee/delete")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userIds" , value="用户Id",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="departmentId" , value="部门id",dataType="String"),
    })
    public JSONMessage webDelEmployee1(@RequestParam String userIds, @RequestParam String departmentId){
        if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        ObjectId departId = new ObjectId(departmentId);

        // 以字符串的形式接收userId，然后解析转换为int
        List<Integer> uIds= new ArrayList<Integer>();
        char first = userIds.charAt(0);
        char last = userIds.charAt(userIds.length() - 1);
        // 用于解析web端
        if(first=='[' && last==']'){
            uIds = JSON.parseArray(userIds, Integer.class);
        }else{
            // 用于解析Android和IOS端
            uIds.add(Integer.parseInt(userIds));
        }

        colleagueManager.deleteEmployee(uIds, departId);
        return JSONMessage.success();
    }

    /**
     * 根据员工ObjectId查询员工信息
     */
    @ApiOperation("查询员工信息")
    @RequestMapping("/employee/msg")
    @ApiImplicitParam(paramType="query" , name="id" , value="员工Id",dataType="String",required=true)
    public JSONMessage employeeMsg(@RequestParam String id){
        Employee employee = colleagueManager.getEmployee(new ObjectId(id));
        return JSONMessage.success(employee);
    }

    /**
     * 查询部门列表
     */
    @ApiOperation("根据公司查询部门信息")
    @RequestMapping("/department/all")
    @ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
    public JSONMessage departmentAll(@RequestParam String companyId){
        List<Department> departments = colleagueManager.departmentAllList(new ObjectId(companyId));
        return JSONMessage.success(departments);
    }

    /**
     * 修改员工信息
     */
    @ApiOperation("修改员工信息")
    @RequestMapping("/update/employee")
    public JSONMessage updataDepartmentMsg(@Valid Employee employee){
        Company company = companyManager.getCompany(employee.getCompanyId());
        if (company.getCreateUserId() == employee.getUserId()){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        Employee empData = colleagueManager.modifyEmpInfo(employee);
        return JSONMessage.success(empData);
    }



    /**
     * 创建部门
     */
    @ApiOperation("创建部门")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="parentId" , value="父Id（上一级部门的Id，根部门的父id为公司Id）",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="departName" , value="部门名称",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="createUserId" , value="创建者userId",dataType="int",required=true)
    })
    @RequestMapping("/add/deparment")
    public JSONMessage addDepartment(@RequestParam String companyId, @RequestParam String parentId, @RequestParam String departName,@RequestParam int createUserId){
        if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }

        Object data = colleagueManager.createDepartment(new ObjectId(companyId), new ObjectId(parentId), departName + "部", createUserId);
        return JSONMessage.success(data);
    }


    /**
     * 修改部门信息
     */
    @ApiOperation("修改部门名称")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="departmentId" , value="部门Id",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="dpartmentName" , value="新部门名称",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="newDepId" , value="移动自新部门",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="oldDpartmentName" , value="旧部门名称",dataType="String",required=true)
    })
    @RequestMapping("/update/department")
    public JSONMessage updateDepartmentName (@RequestParam String departmentId,@RequestParam  String newDpartmentName,@RequestParam String newDepId,@RequestParam String oldDpartmentName){
        if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }

        //检查是否有重名的公司
        if (!newDpartmentName.equals(oldDpartmentName)){
            if(null != companyDao.findOneByName(newDpartmentName)){
                throw new ServiceException(CompanyConstants.ResultCode.DeptNameRepeat);
            }
        }

        Department department = new Department();
        department.setId(new ObjectId(departmentId));
        department.setDepartName(newDpartmentName);
        if (!newDepId.equals("0")){
            department.setParentId(new ObjectId(newDepId));
        }
        Object data = colleagueManager.webModifyDepartmentInfo(department,oldDpartmentName);
        return JSONMessage.success(data);
    }

    /**
     * 删除部门
     */
    @ApiOperation("删除部门")
    @ApiImplicitParam(paramType="query" , name="departmentId" , value="要删除的部门Id",dataType="String",required=true)
    @RequestMapping("/delete/department")
    public JSONMessage deleteDepartment (@RequestParam String departmentId){
        if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        ObjectId departId = new ObjectId(departmentId);
        colleagueManager.deleteDepartment(departId);
        return JSONMessage.success();
    }


    /**
     * 添加员工
     */
    @ApiOperation("添加员工")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="要添加的用户userId集合（json字符串）",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String"),
            @ApiImplicitParam(paramType="query" , name="departmentId" , value="部门Id",dataType="String"),
            @ApiImplicitParam(paramType="query" , name="role" , value="角色值 默认值:1 ,1:普通员工2：管理员 3：创建者",dataType="int")
    })
    @RequestMapping("/web/employee/add")
    public JSONMessage webAddEmployee (@RequestParam String telephone, @RequestParam String companyId,
                                       @RequestParam String departmentId, @RequestParam(defaultValue = "0") int role){
        if(!ObjectId.isValid(companyId)||!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }

        telephone = "86" + telephone;
        //判断该userId是否存在
        User user = userCoreService.getUser(telephone);

        ObjectId compId = new ObjectId(companyId);
        ObjectId departId = new ObjectId(departmentId);
        Object data = colleagueManager.webAddEmployee(compId, departId, user.getUserId(),(byte) role);
        if (ObjectUtil.isEmpty(data)){
            return JSONMessage.success(KConstants.ResultCode.USEREXITS);
        }
        return JSONMessage.success(data);
    }

    /**
     * 查询公司
     */
    @RequestMapping("/web/companyById")
    public JSONMessage webQueryCompanyById (@RequestParam String keyworld,@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit){
        PageResult<Company> data = colleagueManager.findCompanyByKeyworldLimit(keyworld,page,limit);
        return JSONMessage.success(data);
    }


    /**
     * 添加公司
     **/
    @RequestMapping(value = "/web/company/create")
    public JSONMessage createCompany(@RequestParam String companyName){

        try {
            int createUserId = ReqUtil.getUserId();
            if(companyName != null && !"".equals(companyName) && createUserId > 0){
                Company company = colleagueManager.createCompany(companyName, createUserId);
                //将部门及员工数据封装进公司
                company.setDepartments(colleagueManager.departmentList(company.getId()) );
                Object data = company;
                return JSONMessage.success(data);
            }
        } catch (Exception e) {
            return JSONMessage.failureByException(e);

        }
        return JSONMessage.failureByErrCode(CompanyConstants.ResultCode.createCompanyFailure);

    }


    /**
     *  删除公司(即：记录删除者id,将公司信息隐藏)
     */
    @ApiOperation("删除公司")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
    })
    @RequestMapping("/web/company/delete")
    public JSONMessage deleteCompany(@RequestParam String companyId){
        if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        try {
            ObjectId compId = new ObjectId(companyId);
            colleagueManager.deleteCompany(compId, ReqUtil.getUserId());
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }


    /**
     * 后台审核公司接口
     */
    @ApiOperation("审核公司")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
    })
    @RequestMapping("/web/company/check")
    public JSONMessage companyPassCheck(@RequestParam String companyId,@RequestParam byte isCheck){
        if(!ObjectId.isValid(companyId) || ( isCheck != 1 && isCheck !=0 ) ) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        try {
            colleagueManager.checkedCompany(new ObjectId(companyId),isCheck);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }


    /**
     * 新建公司
     */
    @ApiOperation("新建公司")
    @RequestMapping("/create/company")
    public JSONMessage createCompany(Company company){
        colleagueManager.createCompany(company);
        return JSONMessage.success();
    }

}
