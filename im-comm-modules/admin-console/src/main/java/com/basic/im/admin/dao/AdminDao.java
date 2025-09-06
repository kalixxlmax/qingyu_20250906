package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.Admin;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

public interface AdminDao extends IMongoDAO<Admin, ObjectId> {

    void addAdmin(Admin admin);

    Admin getAdminByAccount(String account);

    Admin getAdminById(ObjectId adminId);

    PageResult<Admin> getAdminList(String keyWorld, ObjectId adminId, int pageIndex, int pageSize);

    void deleteAdmin(ObjectId id);

    boolean updateAdminPassword(ObjectId adminId,String newPwd);

    Admin updateAdmin(ObjectId adminId, Map<String,Object> map);

    /**
     * 删除店铺
     **/
    void deleteShop(Long userId);

    /**
     * 删除用户发布商品
     **/
    void deleteProduct(String shopId);

    /**
     * 删除会员
     **/
    void deleteMember(Long userId);
}
