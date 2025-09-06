package com.basic.im.admin.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.WebUrlRosterDao;
import com.basic.im.admin.entity.WebUrlRoster;
import com.basic.im.admin.service.WebUrlRosterManager;
import com.basic.im.user.entity.Report;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.querydsl.QPageRequest;
import org.springframework.stereotype.Service;

import java.net.URL;


@Service
public class WebUrlRosterManagerImpl implements WebUrlRosterManager {

    @Autowired
    private WebUrlRosterDao webUrlRosterDao;


    @Override
    public void addWebUrlRosterRecord(String webUrl, byte urlType) {
        WebUrlRoster webUrlRoster = new WebUrlRoster(webUrl,urlType);
        webUrlRosterDao.addWebUrlRoster(webUrlRoster);
    }




    @Override
    public byte checkWebUrlType(String webUrl) {

        String prefix = "www.";
        String suffixOne = ".com";
        String suffixTwo = ".cn";
        try {
            if(webUrl.startsWith("http")){
                URL requestUrl = new URL(webUrl);
                webUrl = requestUrl.getHost();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 无主机host地址允许通过，eg:第三方分享进来的app协议类的请求
            return 0;
        }

       /* String websiteUrl = "";
        if (webUrl.startsWith(prefix)) {
            websiteUrl = webUrl.replace(prefix, "");
        } else {
            websiteUrl = webUrl;
        }

        String websiteUrl2 = "";
        if (websiteUrl.contains(suffixOne)) {
            websiteUrl2 = websiteUrl.replace(suffixOne, "");
        } else {
            websiteUrl2 = websiteUrl;
        }

        String websiteUrl3 = "";
        if (websiteUrl2.contains(suffixTwo)) {
            websiteUrl3 = websiteUrl2.replace(suffixTwo, "");
        } else {
            websiteUrl3 = websiteUrl2;
        }*/

        //Byte  webUrlType = webUrlRosterDao.queryWebUrlType(websiteUrl3);
        Byte  webUrlType = webUrlRosterDao.queryWebUrlType(webUrl);
        return (null==webUrlType) ? 0 : webUrlType;
    }


    @Override
    public PageResult<WebUrlRoster> getWebUrlRosterList(String webUrl, byte webUrlType, int page, int limit) {
        return webUrlRosterDao.findWebUrlRosterList(webUrl,webUrlType,page,limit);
    }

    @Override
    public void addWebUrlRoster(String webUrl, byte webUrlType) {
        WebUrlRoster webUrlRoster = new WebUrlRoster(webUrl,webUrlType);
        webUrlRosterDao.addWebUrlRoster(webUrlRoster);
    }

    @Override
    public  void  deleteWebUrlRoster(ObjectId webUrlRostreId){
        webUrlRosterDao.deleteById(webUrlRostreId);
    }


    @Override
    public void updateWebUrlRosterStatus(ObjectId webUrlRostreId, byte webUrlType) {

    }
}
