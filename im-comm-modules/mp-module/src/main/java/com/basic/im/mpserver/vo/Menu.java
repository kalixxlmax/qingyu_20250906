package com.basic.im.mpserver.vo;

import com.basic.im.comm.utils.IdWorker;
import com.basic.im.mpserver.model.MenuVO;
import com.basic.utils.StringUtil;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;


@Data
@Document(value = "mp_menu")
public class Menu {

	private @Id String id; //记录id

	private String apiName; //用来标识某一个接口

	@Indexed
	private  String parentId = "0";

	@Indexed
	private int userId;

	@Indexed
	private int index;

	private String name = "";

	private String desc = "";

	private String url = "";  //链接url

	private boolean haveChild; //是否有子集

	@Transient
	private List<Menu>  menuList;

	public Menu() { }

	public Menu(MenuVO menuVO) {
		this.id = IdWorker.getId() + "";
		this.parentId = menuVO.getParentId();
		this.userId = menuVO.getUserId();
		this.index = menuVO.getIndex();
		this.name = menuVO.getName();
		this.desc = menuVO.getDesc();
		this.url = menuVO.getUrl();
		this.apiName = menuVO.getApiName();
		this.haveChild = false; //默认为false
	}
}
