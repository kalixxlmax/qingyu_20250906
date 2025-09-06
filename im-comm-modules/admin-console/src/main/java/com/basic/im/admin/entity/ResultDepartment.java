package com.basic.im.admin.entity;

import com.basic.im.comm.constants.KConstants;
import lombok.Data;

import java.util.List;

@Data
public class ResultDepartment {

	private String id;

	/**
	 * 名称
	 */
	private String name;

	/**
	 * 员工角色
	 * 0：普通员工     1：部门管理者(暂时没用)    2：公司管理员    3：公司创建者
	 */
	private int role = -1;

	/**
	 * 头衔
	 */
	private String position;

	/**
	 * 是否客服
	 *  0:不是  1:是
	 */
	private int isCustomer = -1;

	/**
	 * 用户编号
	 */
	private int userId;

	/**
	 * 上级编号
	 */
	private String parentId;


	private int isMenu = 0;


	private List<ResultDepartment> children;


	public ResultDepartment(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public ResultDepartment(String id, String name, List<ResultDepartment> children) {
		this.id = id;
		this.name = name;
		this.children = children;
	}

	public ResultDepartment(String id, String name, int role, String position, int isCustomer, int userId, String parentId) {
		this.id = id;
		this.name = name;
		this.role = role;
		this.position = position;
		this.isCustomer = isCustomer;
		this.userId = userId;
		this.parentId = parentId;
		this.isMenu = KConstants.ONE;
	}
}
