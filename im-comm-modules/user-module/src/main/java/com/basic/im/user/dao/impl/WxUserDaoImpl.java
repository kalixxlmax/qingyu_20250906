package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.WxUserDao;
import com.basic.im.user.entity.WxUser;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class WxUserDaoImpl extends MongoRepository<WxUser,Integer> implements WxUserDao {


    @Override
    public Class<WxUser> getEntityClass() {
        return WxUser.class;
    }

    @Override
    public WxUser addWxUser(Integer userId, String openId, String nickname, String imgurl, int sex, String city, String country, String province) {
        WxUser wxUser = new WxUser();
        wxUser.setWxuserId(userId);
        wxUser.setOpenId(openId);
        wxUser.setNickname(nickname);
        wxUser.setImgurl(imgurl);
        wxUser.setSex(sex);
        wxUser.setCity(city);
        wxUser.setCountry(country);
        wxUser.setProvince(province);
        wxUser.setCreatetime(DateUtil.currentTimeSeconds());
        getDatastore().save(wxUser);
        return wxUser;
    }

    @Override
    public WxUser getWxUser(String openid, Integer userId) {
        Query query = createQuery();
        if (!StringUtil.isEmpty(openid)) {
            addToQuery(query,"openId",openid);
        }
        if (null != userId){
            addToQuery(query,"wxuserId",userId);
        }
        return findOne(query);
    }
}
