package com.basic.im.mpserver.dao.impl;

import com.mongodb.client.result.UpdateResult;
import com.basic.im.mpserver.dao.MenuDao;
import com.basic.im.mpserver.vo.Menu;
import com.basic.im.repository.MongoRepository;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/24 16:59
 */
@Repository
public class MenuDaoImpl extends MongoRepository<Menu,String> implements MenuDao {

   
    @Override
    public Class<Menu> getEntityClass() {
        return Menu.class;
    }

    @Override
    public void addMenu(Menu menu) {
        getDatastore().save(menu);
    }

    @Override
    public List<Menu> getMenuList(int userId, String parentId) {
        Query query = createQuery("userId",userId);
                addToQuery(query,"parentId",parentId);
                //ascByquery(query,"index");
        return queryListsByQuery(query);
    }

    @Override
    public Menu getMenuByParentId(String parentId) {
        Query query = createQuery("parentId",parentId);
        return findOne(query);
    }

    @Override
    public List<Menu> getMenuList(String parentId) {
        Query query = createQuery("parentId",parentId);
        ascByquery(query,"index");
        return queryListsByQuery(query);
    }

    @Override
    public void updateMenu(String menuId, Map<String, Object> map) {
        Query query = createQuery(menuId);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        getDatastore().updateFirst(query,ops,getEntityClass());
    }


    @Override
    public boolean updateMenuHaveChild(String menuId) {
        //检查该菜单是否存在子级
        Query query = createQuery("parentId",menuId);

        Update ops = createUpdate();
        ops.set("haveChild", (null != findOne(query)) ? true : false);

        //更新是否存在子集的值
       UpdateResult updateResult = update(createQuery(menuId),ops);
        return  updateResult.getModifiedCount() > 0;
    }

    @Override
    public void deleteMenu(String menuId) {
       deleteById(menuId);
    }

    @Override
    public boolean isHaveChild(String id) {
        Query query = createQuery("parentId",id);
        Menu menu = findOne(query);
        if (null == menu){
            return  false;
        }
        return  true;
    }
}
