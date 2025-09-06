package com.basic.im.api.controller;

import com.alibaba.fastjson.JSON;
import com.basic.common.model.PageResult;
import com.basic.im.api.AbstractController;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.LoginPassword;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.company.CompanyConstants;
import com.basic.im.company.entity.Company;
import com.basic.im.company.entity.Department;
import com.basic.im.company.entity.Employee;
import com.basic.im.company.service.impl.CompanyManagerImpl;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.service.AuthKeysService;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

/**
 * 用于组织架构功能的相关接口
 * @author hsg
 *
 */
@Slf4j
@Api(value="CompanyController",tags="用于组织架构功能的相关接口")
@RestController
@RequestMapping(value = "/org",method={RequestMethod.GET,RequestMethod.POST})
public class CompanyController extends AbstractController {

	@Autowired
	private CompanyManagerImpl companyManager;

	// 创建公司
	@ApiOperation("创建公司 ")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="companyName" , value="公司名称",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="createUserId" , value="创建者userId",dataType="int",required=true)
	})
	@RequestMapping(value = "/company/create")
	public JSONMessage createCompany(@RequestParam String companyName, @RequestParam int createUserId){

		try {
			if(StringUtil.isEmpty(companyName) ||  createUserId <= 0){
				return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
			}
			Company company = companyManager.createCompany(companyName, createUserId);

			//公司是否通过审核
			if(0==company.getIsChecked()){
				return JSONMessage.failureByErrCode(KConstants.ResultCode.DataAlreadSubmitWaitedChecked);
			}

			company.setDepartments(companyManager.departmentList(company.getId(),(byte)1) ); //将部门及员工数据封装进公司

			return JSONMessage.success(company);
		} catch (Exception e) {
			return JSONMessage.failureByException(e);
		}

	}


	// 根据userId查找是否存在其所属的公司
	@ApiOperation("根据userId查找其所属的公司")
	@RequestMapping("/company/getByUserId")
	public JSONMessage getCompanyByUserId(){
		List<Company> companies = companyManager.findCompanyByUserId(ReqUtil.getUserId());
		if (companies == null || companies.isEmpty()){  //判断是否存在公司
			return JSONMessage.success();
		}
		for (Company company : companies) {   //遍历公司赋值临时数据
			company.setDepartments(companyManager.departmentList(company.getId(),(byte)1));  // 将部门及员工数据封装进公司
		}
		return JSONMessage.success(companies);
	}


	@ApiOperation("查找公司:（通过公司名称的关键字查找）")
	@RequestMapping("/company/search")
	public JSONMessage searchCompany(@RequestParam(defaultValue = "") String keyword){
		List<Company> companies = companyManager.findCompanyByKeyworld(keyword);
		return JSONMessage.success(companies);
	}


	@ApiOperation("申请加入公司")
	@RequestMapping("/company/applyJoin")
	public JSONMessage applyJoinCompany(@RequestParam(defaultValue = "") String companyId,
										@RequestParam(defaultValue = "") String departmentId,
										@RequestParam(defaultValue = "") String reason){

		if(!ObjectId.isValid(companyId)) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}

		try {
			companyManager.applyJoinCompany(new ObjectId(companyId), departmentId, reason, ReqUtil.getUserId());
			return JSONMessage.success();
		} catch (Exception e) {
			return JSONMessage.failureByException(e);
		}
	}


	@ApiOperation("创建者确认某个用户加入公司")
	@RequestMapping("/company/manager/confirm/join")
	public JSONMessage joinCompany(@RequestParam(defaultValue = "") String companyId, @RequestParam(defaultValue = "") String departmentId,
									@RequestParam int joinUserId){

		if(!ObjectId.isValid(companyId)||!ObjectId.isValid(departmentId)) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}

		ObjectId objCompanyId = new ObjectId(companyId);

		//验证权限,本公司创建者可以调用
		if(!companyManager.verifyAuthByCompanyId(objCompanyId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_CREATER)) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
		}
		try {
			Object data = companyManager.managerConfirmjoinCompany(objCompanyId, new ObjectId(departmentId), joinUserId);

			return JSONMessage.success(data);

		} catch (Exception e) {
			return JSONMessage.failureByException(e);
		}
	}

	@ApiOperation("邀请用户加入公司")
	@RequestMapping("/company/invite/user/join")
	public  JSONMessage  inviteUsersJoinCompany(@RequestParam String  companyId,@RequestParam String  departmentId ,@RequestParam String inviteUserIdStr){

		if(!ObjectId.isValid(companyId)) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		// 以字符串的形式接收userId，然后解析转换为int
		List<Integer> inviteUserIds = new ArrayList<Integer>();

		for(String str : inviteUserIdStr.split(",|，")){
			if(str != null && !"".equals(str)){
				inviteUserIds.add(Integer.parseInt(str));
			}
		}
		companyManager.inviteUsersJoinCompany(new ObjectId(companyId),departmentId,ReqUtil.getUserId(), inviteUserIds);
		return JSONMessage.success();
	}


	@ApiOperation("用户确认加入公司")
	@RequestMapping("/company/user/confirm/join")
	public  JSONMessage  userConfirmJoinCompany(@RequestParam String  companyId, @RequestParam String  departmentId){

		if(!ObjectId.isValid(companyId)||!ObjectId.isValid(departmentId)) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		companyManager.userConfirmJoinCompany(new ObjectId(companyId) ,  new ObjectId(departmentId),ReqUtil.getUserId());
		return JSONMessage.success();
	}



	/**
	 * 指定管理员
	 * @param companyId  公司id
	 * @param managerId  管理员 id 列表  [1002021,10021212]
	 * @return
	 */
	@RequestMapping("/company/setManager")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="managerId" , value="管理员 id 列表",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	})
	@ApiOperation("设置公司管理员")
	public JSONMessage setCompanyManager(@RequestParam String companyId, @RequestParam String managerId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }

		ObjectId compId = new ObjectId(companyId);
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_CREATER))
			// 需要创建者权限
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		//以字符串的形式接收managerId，然后解析转换为int
		List<Integer> userIds= new ArrayList<Integer>();
		if(managerId.charAt(0)=='[' && managerId.charAt(managerId.length() - 1)==']'){
			userIds = JSON.parseArray(managerId, Integer.class);
		}
		companyManager.setManager(compId, userIds);
		return JSONMessage.success();
	}


	//管理员列表
	@ApiOperation("管理员列表 ")
	@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	@RequestMapping("/company/managerList")
	public JSONMessage managerList(@RequestParam String companyId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证,公司成员可以调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 需要创建者权限
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Object data = companyManager.managerList(compId);
		return JSONMessage.success(data);
	}

	// 修改公司名称、公告
	@ApiOperation("修改公司名称、公告")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
			@ApiImplicitParam(paramType="query" , name="companyName" , value="要修改的公司名称",dataType="String"),
			@ApiImplicitParam(paramType="query" , name="noticeContent" , value="要修改的公告内容",dataType="String",defaultValue = "")
	})
	@RequestMapping("/company/modify")
	public JSONMessage modifyCompany(@RequestParam String companyId, String companyName,@RequestParam(defaultValue = "") String noticeContent){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);

		Company company = new Company();
		company.setId(compId);
		if(companyName != null){
			//权限验证,公司创建者才能修改名称
			if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_CREATER))
				// 需要创建者权限
            {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
            }
			company.setCompanyName(companyName);
		}else if(noticeContent != null &&  !"".equals(noticeContent)){ //判断是否存在公告
			//权限验证,公司管理员以上才能修改公告
			if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER))
				// 需要管理员以上权限才能修改公告
            {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
            }
			company.setNoticeContent(noticeContent);
			company.setNoticeTime(DateUtil.currentTimeSeconds());
		}
		Object data = companyManager.modifyCompanyInfo(company);
		return JSONMessage.success(data);
	}



	// 删除公司(即：记录删除者id,将公司信息隐藏)
	@ApiOperation("删除公司")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	})
	@RequestMapping("/company/delete")
	public JSONMessage deleteCompany(@RequestParam String companyId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		try {
			ObjectId compId = new ObjectId(companyId);
			companyManager.deleteCompany(compId, ReqUtil.getUserId());
			return JSONMessage.success();
		} catch (Exception e) {
			return JSONMessage.failureByException(e);
		}
	}


	// 创建部门
	@ApiOperation("创建部门")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="parentId" , value="父Id（上一级部门的Id，根部门的父id为公司Id）",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="departName" , value="部门名称",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="createUserId" , value="创建者userId",dataType="int",required=true)
	})
	@RequestMapping("/department/create")
	public JSONMessage createDepartment(@RequestParam String companyId, @RequestParam String parentId, @RequestParam String departName,@RequestParam int createUserId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		if(ReqUtil.getUserId()!=createUserId) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		ObjectId compId = new ObjectId(companyId);
		//权限验证,需要公司管理员及以上权限
		if(!companyManager.verifyAuthByCompanyId(compId, createUserId, CompanyConstants.ROLE.COMPANY_MANAGER))
			// 需要公司管理员及以上权限
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		ObjectId parentID = new ObjectId();
		if(parentId.trim() != null){
			parentID = new ObjectId(parentId);
		}
		Object data = companyManager.createDepartment(compId, parentID, departName, createUserId);
		return JSONMessage.success(data);
	}

	// 修改部门名称
	@ApiOperation("修改部门名称")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="departmentId" , value="部门Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="dpartmentName" , value="部门名称",dataType="String",required=true)
	})
	@RequestMapping("/department/modify")
	public JSONMessage modifyDepartment (@RequestParam String departmentId,@RequestParam  String dpartmentName){
		if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId departId = new ObjectId(departmentId);
		//权限验证,需要公司管理员及以上权限
		if(!companyManager.verifyAuthByDepartmentId(departId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Department department = new Department();
		department.setId(departId);
		department.setDepartName(dpartmentName);
		Object data = companyManager.modifyDepartmentInfo(department);
		return JSONMessage.success(data);
	}


	// 删除部门
	@ApiOperation("删除部门")
	@ApiImplicitParam(paramType="query" , name="departmentId" , value="要删除的部门Id",dataType="String",required=true)
	@RequestMapping("/department/delete")
	public JSONMessage modifyDepartment (@RequestParam String departmentId){
		if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId departId = new ObjectId(departmentId);
		//权限验证,需要公司管理员及以上权限
		if(!companyManager.verifyAuthByDepartmentId(departId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		companyManager.deleteDepartment(departId);
		return JSONMessage.success();
	}

	// 添加员工
	@ApiOperation("添加员工")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="userId" , value="要添加的用户userId集合（json字符串）",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String"),
		@ApiImplicitParam(paramType="query" , name="departmentId" , value="部门Id",dataType="String"),
		@ApiImplicitParam(paramType="query" , name="role" , value="角色值 默认值:1 ,1:普通员工2：管理员 3：创建者",dataType="int")
	})
	@RequestMapping("/employee/add")
	public JSONMessage addEmployee (@RequestParam String userId, @RequestParam String companyId,
			              @RequestParam String departmentId){
		if(!ObjectId.isValid(companyId)||!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }

		ObjectId compId = new ObjectId(companyId);

		//验证权限,本公司员工可以调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		// 以字符串的形式接收userId，然后解析转换为int
		List<Integer> userIds= new ArrayList<Integer>();
		char first = userId.charAt(0);
		char last = userId.charAt(userId.length() - 1);
		if(first=='[' && last==']'){  // 用于解析web端
			userIds = JSON.parseArray(userId, Integer.class);
		}else{ // 用于解析Android和IOS端
			String[] strs = userId.split(",");
			for(String str : strs){
				if(str != null && !"".equals(str)){
					userIds.add(Integer.parseInt(str));
				}
			}
		}

		ObjectId departId = new ObjectId(departmentId);
		Object data = companyManager.addEmployee(compId, departId, userIds);
		return JSONMessage.success(data);
	}


	// 删除员工
	@ApiOperation("删除员工")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="userIds" , value="要删除的用户userId集合（json字符串）",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="departmentId" , value="部门id",dataType="String",required=true)
	})
	@RequestMapping("/employee/delete")
	public JSONMessage delEmployee (@RequestParam String userIds, @RequestParam String departmentId){
		if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId departId = new ObjectId(departmentId);
		//验证权限,公司管理员及其以上可以调用
		if(!companyManager.verifyAuthByDepartmentId(departId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		// 以字符串的形式接收userId，然后解析转换为int
		List<Integer> uIds= new ArrayList<Integer>();
		char first = userIds.charAt(0);
		char last = userIds.charAt(userIds.length() - 1);
		if(first=='[' && last==']'){ // 用于解析web端
			uIds = JSON.parseArray(userIds, Integer.class);
		}else{ // 用于解析Android和IOS端
			uIds.add(Integer.parseInt(userIds));
		}

		companyManager.deleteEmployee(uIds, departId);
		return JSONMessage.success();
	}

	// 更改员工部门
	@ApiOperation("更改员工部门")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="userId" , value="要更改的员工的userId",dataType="int",required=true),
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="newDepartmentId" , value="要更改的部门Id",dataType="String",required=true)
	})
	@RequestMapping("/employee/modifyDpart")
	public JSONMessage addEmployee (@RequestParam int userId, @RequestParam String companyId, @RequestParam String newDepartmentId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);

		//验证权限,公司管理员及其以上可以调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		// 员工换不能职位不变
		Employee employeeByUserId = companyManager.getEmployeeByUserId(compId, userId);
		Employee employee = new Employee();
		employee.setPosition(employeeByUserId.getPosition());
		employee.setCompanyId(compId);
		employee.setUserId(userId);
		employee.setDepartmentId(new ObjectId(newDepartmentId));
		employee.setId(ObjectId.get());
		Company company = companyManager.getCompany(compId);
		if(company.getCreateUserId() == userId){
			employee.setRole(CompanyConstants.ROLE.COMPANY_CREATER);
		}
		Object data = companyManager.updateDeparmen(employee);
//		Object data = companyManager.modifyEmpInfo(employee);  //更改该员工的信息

		return JSONMessage.success(data);
	}

	/**
	* @Title: updateEmployee
	* @Description: 更改员工信息
	* @param @param employee
	* @param @return    参数
	* @return JSONMessage    返回类型
	* @throws
	*/

	@ApiOperation("更改员工信息")
	@RequestMapping("/employee/updateEmployee")
	public JSONMessage updateEmployee(@Valid Employee employee){
		JSONMessage jsonMessage = JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);

		if(ReqUtil.getUserId()!=employee.getUserId()) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(employee.getCompanyId(), ReqUtil.getUserId(),CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		try {
			Employee employeeInfo = companyManager.modifyEmpInfo(employee);
			if (!StringUtil.isEmpty(employeeInfo.toString())) {
				return JSONMessage.success("", employeeInfo);
			}else{
				return jsonMessage;
			}
		} catch (Exception e) {
			return JSONMessage.failureByException(e);
		}
	}

	// 部门员工列表
	@ApiOperation("部门员工列表 ")
	@ApiImplicitParam(paramType="query" , name="departmentId" , value="部门Id",dataType="String",required=true)
	@RequestMapping("/departmemt/empList")
	public JSONMessage departEmpList (@RequestParam String departmentId){
		if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId departId = new ObjectId(departmentId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByDepartmentId(departId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Object data = companyManager.departEmployeeList(departId);
		return JSONMessage.success(data);
	}

	//公司员工列表
	@ApiOperation("公司员工列表")
	@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	@RequestMapping("/company/employees")
	public JSONMessage companyEmpList(@RequestParam String companyId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Object data = companyManager.employeeList(compId);
		return JSONMessage.success(data);
	}

	// 更改员工角色
	@ApiOperation("更改员工角色")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="userId" , value="用户Id",dataType="int",required=true),
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String"),
		@ApiImplicitParam(paramType="query" , name="role" , value="角色值",dataType="int")
	})
	@RequestMapping("/employee/modifyRole")
	public JSONMessage addEmployee (@RequestParam int userId, @RequestParam String companyId, @RequestParam byte role){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMPANY_MANAGER)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Employee employee = new Employee();
		employee.setCompanyId(compId);
		employee.setUserId(userId);
		employee.setRole(role);
		Object data = companyManager.modifyEmpInfo(employee);
		return JSONMessage.success(data);
	}

	// 更改员工职位(头衔)
	@ApiOperation("更改员工职位(头衔)")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="userId" , value="用户Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="position" , value="职位名称",dataType="String",required=true)
	})
	@RequestMapping("/employee/modifyPosition")
	public JSONMessage modifyPosition (@RequestParam String companyId, @RequestParam String position, @RequestParam(defaultValue="0") int userId){
		if (0 == userId) {
            userId = ReqUtil.getUserId();
        }
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, userId, CompanyConstants.ROLE.COMMON_EMPLOYEE))
			//权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }

		Employee employee = new Employee();
		employee.setCompanyId(compId);
		employee.setUserId(userId);
		int empRole = companyManager.getEmpRole(compId, userId);
		employee.setRole((byte) empRole);
		employee.setPosition(position);
		Object data = companyManager.modifyEmpInfo(employee);
		return JSONMessage.success(data);
	}


	//公司列表
	@RequestMapping("/company/list")
	public JSONMessage companyList ( @RequestParam(defaultValue = "0") int page,@RequestParam(defaultValue = "15") int limit,
									@RequestParam(defaultValue = "") String keyword){
		PageResult<Company> data = companyManager.companyList(limit, page,keyword);
		return JSONMessage.success(data);
	}

	// 部门列表
	@ApiOperation("部门列表")
	@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	@RequestMapping("/department/list")
	public JSONMessage departmentList (@RequestParam String companyId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);

		Object data = null;
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 非本公司员工不返回员工数据
        {
            //return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
			data = companyManager.departmentList(compId,(byte)0);
		}else {
			data = companyManager.departmentList(compId,(byte)1);
		}

		return JSONMessage.success(data);
	}

	// 获取公司详情
	@ApiOperation("获取公司详情 ")
	@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true)
	@RequestMapping("/company/get")
	public JSONMessage getCompany (@RequestParam String companyId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		/*//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }*/
		Object data = companyManager.getCompany(compId);
		return JSONMessage.success(data);
	}



	// 获取部门
	@ApiOperation("获取部门")
	@ApiImplicitParam(paramType="query" , name="departmentId" , value="公司Id",dataType="String",required=true)
	@RequestMapping("/department/get")
	public JSONMessage getDpartment(@RequestParam String departmentId){
		if(!ObjectId.isValid(departmentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId departId = new ObjectId(departmentId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByDepartmentId(departId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Object data = companyManager.getDepartmentVO(departId);
		return JSONMessage.success(data);
	}

	// 员工退出公司
	@ApiOperation("员工退出公司 ")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
		@ApiImplicitParam(paramType="query" , name="userId" , value="用户Id",dataType="int",required=true)
	})
	@RequestMapping("/company/quit")
	public JSONMessage quitCompany(@RequestParam String companyId, @RequestParam int userId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		companyManager.empQuitCompany(compId, userId);
		return JSONMessage.success();
	}

	// 获取公司中某个员工角色值
	@ApiOperation("获取公司中某个员工角色值")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType="query" , name="companyId" , value="公司Id",dataType="String",required=true),
			@ApiImplicitParam(paramType="query" , name="userId" , value="员工的userId",dataType="String")
	})
	@RequestMapping("/employee/role")
	public JSONMessage getEmployRole(@RequestParam String companyId, @RequestParam int userId){
		if(!ObjectId.isValid(companyId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
		ObjectId compId = new ObjectId(companyId);
		//权限验证,本公司员工才能调用
		if(!companyManager.verifyAuthByCompanyId(compId, ReqUtil.getUserId(), CompanyConstants.ROLE.COMMON_EMPLOYEE))
			// 权限不足,非本公司员工
        {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
		Object data = companyManager.getEmpRole(compId, userId);
		return JSONMessage.success(data);
	}

	@ApiOperation("获取公司同事的Id列表")
	@RequestMapping("/employee/idlist")
	public JSONMessage idlist(){

		Integer userId = ReqUtil.getUserId();

		ObjectId compId = companyManager.queryCompanyId(userId);

		Object data = companyManager.queryEmployeeUserIdList(compId);
		return JSONMessage.success(data);
	}

	@ApiOperation("查询是否为同事关系")
	@ApiImplicitParam(paramType="query" , name="userId" , value="同事的userId",dataType="int")
	@RequestMapping("/company/isWorkmate")
	public JSONMessage isWorkmate(@RequestParam(defaultValue = "0") int userId){

		Integer myuserId = ReqUtil.getUserId();

		ObjectId compId = companyManager.queryCompanyId(myuserId);
		if(0==userId){
			return JSONMessage.failure(null);
		}
		if(!companyManager.verifyAuthByCompanyId(compId, userId, CompanyConstants.ROLE.COMMON_EMPLOYEE)){
			// 权限不足,非本公司员工
			return JSONMessage.failure(null);
		}
		return JSONMessage.success();
	}



	@Autowired
	private AuthKeysService authKeysService;


	@Autowired
	private FriendsManagerImpl friendsManager;

	@Lazy
	@Autowired(required = false)
	private RoomManagerImplForIM roomManagerIM;



	@RequestMapping("/cleanFriendsSecretMsg")
	public JSONMessage cleanFriendsSecretMsg(@RequestParam(defaultValue = "") String password) {
		Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());

		String dbPwd = authKeysService.queryLoginPassword(userId);
		if (StringUtil.isEmpty(password) || (!password.equals(dbPwd)&&
				!LoginPassword.encodeFromOldPassword(dbPwd).equals(password))) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.VERIFYPASSWORDFAIL);
		}

		log.info("cleanFriendsSecretMsg ===> {}",userId);
		ObjectId companyId = companyManager.queryCompanyId(userId);
		if(null != companyId){
			List<Integer> userIdList = companyManager.queryEmployeeUserIdList(companyId);
			friendsManager.cleanFriendsSecretMsg(userIdList,userId);
		}
		//设置好友之间的消息为端到端加密，消息过期销毁时长为1天
		friendsManager.ModifyFriendsChatSercetData();

		return JSONMessage.success();
	}



	@RequestMapping("/destroyFlagRoomMsg")
	public JSONMessage destroyFriendsAllMsg(@RequestParam(defaultValue = "") String password) {
		Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());

		String dbPwd = authKeysService.queryLoginPassword(userId);
		if (StringUtil.isEmpty(password) ||(!password.equals(dbPwd)&&
				!LoginPassword.encodeFromOldPassword(dbPwd).equals(password))) {
			return JSONMessage.failureByErrCode(KConstants.ResultCode.VERIFYPASSWORDFAIL);
		}
		log.info("destroyFlagRoomMsg ===> {}",userId);

		ObjectId companyId=companyManager.queryCompanyId(userId);

		if(null!=companyId){
			List<Integer> companyUserIdList = companyManager.queryEmployeeUserIdList(companyId);
			roomManagerIM.destroyFlagRoom(userId,companyUserIdList);
			//friendsManager.cleanFriendsAllMsg(companyUserIdList,userId);

		}
		return JSONMessage.success();
	}

}
