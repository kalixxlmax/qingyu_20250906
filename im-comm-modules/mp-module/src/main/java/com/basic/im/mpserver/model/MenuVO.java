package com.basic.im.mpserver.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

import java.util.List;


@Data
public class MenuVO {


	private String id; //记录Id

	private String parentId;

	private String apiName;  //接口名

	private int userId;

	private int index;

	private String name; //菜单名

	private String desc;

	private String url;

	private List<MenuVO> menuList;

	//是否有子级
	@JSONField(name="haveChild")
	private boolean haveChild;

	public MenuVO() {
		//  Auto-generated constructor stub
	}


	public MenuVO(String id, String parentId, String apiName, int userId, int index, String name, String desc,
				  String url, List<MenuVO> menuList) {
		this.id = id;
		this.parentId = parentId;
		this.apiName = apiName;
		this.userId = userId;
		this.index = index;
		this.name = name;
		this.desc = desc;
		this.url = url;
		this.menuList = menuList;
	}



}
