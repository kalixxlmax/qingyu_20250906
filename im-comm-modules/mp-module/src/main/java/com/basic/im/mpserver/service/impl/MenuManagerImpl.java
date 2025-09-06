package com.basic.im.mpserver.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.IdWorker;
import com.basic.im.friends.dao.FriendsDao;
import com.basic.im.mpserver.dao.MenuDao;
import com.basic.im.mpserver.model.MenuVO;
import com.basic.im.mpserver.service.MenuManager;
import com.basic.im.mpserver.vo.Menu;
import com.basic.im.user.dao.UserDao;
import com.basic.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.util.*;


@Service
public class MenuManagerImpl implements MenuManager {

	@Autowired
	private MenuDao menuDao;

	@Autowired
	private FriendsDao friendsDao;

	@Autowired
	private UserDao userDao;

	
	public List<Menu> getMenuList(int userId){
		List<Menu> data = menuDao.getMenuList(userId,"0");

		if (null != data && data.size() > 0) {
			for (Menu menu : data) {
				List<Menu> urlList = menuDao.getMenuList(menu.getId());
				for(Menu urlMenu : urlList){
					if(!StringUtil.isEmpty(urlMenu.getUrl())) {
                        urlMenu.setUrl(urlMenu.getUrl().replaceAll(" ", ""));
                    }
				}
				menu.setMenuList(urlList);
			}
		}
		return data;
	} 


	public List<Menu> getMenuListByParentId(int userId, String parentId){
		List<Menu> menuList = menuDao.getMenuList(userId,parentId);

		if (null != menuList && menuList.size() > 0) {
			for (Menu menu : menuList) {
				menu.setMenuList(menuDao.getMenuList(menu.getId()));
			}
		}
		return menuList;
	}

	
	@SuppressWarnings("deprecation")
	public JSONObject getHomeCount(int userId) {
		JSONObject obj = new JSONObject();
		long fansCount = friendsDao.getFriendsCount(userId);
		long userCount = userDao.getAllUserCount();
		BasicDBObject query = new BasicDBObject("direction", 0);
		query.append("receiver", userId);
		query.append("isRead", 0);
		obj.put("fansCount", fansCount);
		obj.put("userCount", userCount);
		return obj;
	}


	public void addMenu(MenuVO menuVO) {
		Menu entity = new Menu(menuVO);

		menuDao.addMenu(entity);
		//存在父级id
		if(!"0".equals(menuVO.getParentId()) ){
			menuDao.updateMenuHaveChild(menuVO.getParentId());
		}

	}


	public void  delMenu(String menuId){
		Menu menu = menuDao.get(menuId);

		if(menu == null){
			throw new ServiceException(KConstants.ResultCode.DataNotExists);
		}

		if(null != menuDao.getMenuByParentId(menu.getId()) ){
			throw new ServiceException("存在下级菜单，请先删除下级菜单");
		}

		menuDao.deleteMenu(menuId);

		menuDao.updateMenuHaveChild(menu.getParentId());
	}
	


	public void saveupdate(Menu entity) {
		Map<String,Object> map = new HashMap<>(6);
		if (!"0".equals(entity.getParentId()) ) {
            map.put("parentId", entity.getParentId());
        }
		if (!StringUtil.isEmpty(entity.getName())) {
            map.put("name", entity.getName());
        }

		map.put("url", entity.getUrl());
		if (0 != entity.getIndex()) {
            map.put("index", entity.getIndex());
        }
		if (!StringUtil.isEmpty(entity.getDesc())) {
            map.put("desc", entity.getDesc());
        }
		if(!StringUtil.isEmpty(entity.getApiName())) {
            map.put("apiName", entity.getApiName());
        }
		menuDao.updateMenu(entity.getId(),map);
	}
	
}
