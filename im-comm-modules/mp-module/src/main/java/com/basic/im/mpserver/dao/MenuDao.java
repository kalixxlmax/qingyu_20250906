package com.basic.im.mpserver.dao;

import com.basic.im.mpserver.vo.Menu;
import com.basic.im.repository.IMongoDAO;

import java.util.List;
import java.util.Map;

public interface MenuDao extends IMongoDAO<Menu, String> {

    void addMenu(Menu menu);

    List<Menu> getMenuList(int userId, String parentId);

    Menu getMenuByParentId(String parentId);

    List<Menu> getMenuList(String parentId);

    void updateMenu(String menuId, Map<String, Object> map);

    boolean updateMenuHaveChild(String menuId);

    void deleteMenu(String menuId);

    boolean isHaveChild(String id);

}
