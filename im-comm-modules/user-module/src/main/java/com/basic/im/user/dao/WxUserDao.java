package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.WxUser;

public interface WxUserDao extends IMongoDAO<WxUser,Integer> {

    WxUser addWxUser(Integer userId,String openId,String nickname,String imgurl,int sex,String city,String country,String province);

    WxUser getWxUser(String openid, Integer userId);
}
