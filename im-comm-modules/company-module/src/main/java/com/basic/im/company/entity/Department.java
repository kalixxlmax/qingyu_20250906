package com.basic.im.company.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@Document(value = "department")
public class Department {

	/**
	 * 部门id
	 */
	private @Id ObjectId id;

	/**
	 * 公司id，表示该部门所属的公司
	 */
	private @Indexed ObjectId companyId;

	/**
	 * parentId 表示上一级的部门ID
	 */
	private @Indexed ObjectId parentId;

	/**
	 * 部门名称
	 */
	private @Indexed String departName;

	/**
	 * 创建者userId
	 */
	private @Indexed int createUserId;

	/**
	 * 创建时间
	 */
	private long createTime;

	/**
	 * 部门总人数
	 */
	private int empNum = -1;

	/**
	 * 解决我的同事问题
	 */
	private String parentId1;

	/**
	 * 类型值  0:普通部门  1:根部门  2:分公司    5:默认加入的部门  6.客服部门
	 */
	private @Indexed int type = 0;


	/**
	 * 此属性用于封装部门员工列表
	 */
	private @Transient List<Employee> employees;


	/**
	 * 此属性用于封装该部门的子部门
	 */
	private @Transient List<Department> childDepartment;


	/**
	 * 可以放 直接主管的手机号、邮箱、userID
	 */
	@ApiModelProperty("直接主管")
	private String departmentDirector;
}
