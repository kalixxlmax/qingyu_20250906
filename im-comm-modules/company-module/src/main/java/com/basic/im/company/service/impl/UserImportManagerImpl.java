package com.basic.im.company.service.impl;

import com.basic.im.company.dao.UserImportDao;
import com.basic.im.company.entity.UserImportExample;
import com.basic.im.company.service.UserImportManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description:
 * @Author wxm
 * @Date 2021/4/23 10:36
 */
@Service
public class UserImportManagerImpl  implements UserImportManager {
    @Autowired
    private UserImportDao userImportDao;


    @Override
    public boolean ImportUser(UserImportExample user) {
        UserImportExample temp=userImportDao.ImportUser(user);
        if(temp!=null){
            System.out.println("mongodb 导入excel数据成功");
            return true;
        }
        else {
            System.out.println("mongodb 导入excel数据失败");
            return false;
        }
    }
}
