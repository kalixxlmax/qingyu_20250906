package com.basic.im.security.service.Impl;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.security.dao.ResourceInfoDao;
import com.basic.im.security.dto.ResourceInfoDTO;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.service.ResourceInfoManager;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Service
public class ResourceInfoManagerImpl implements ResourceInfoManager {

    @Autowired
    private ResourceInfoDao resourceInfoDao;

    @Override
    public List<ResourceInfo> queryResourceInfo() {
        return resourceInfoDao.queryResourceInfo();
    }

    @Override
    public void delResourceInfo(ObjectId id) {
        resourceInfoDao.delResourceInfo(id);
    }

    @Override
    public void updateResourceInfo(ResourceInfo resourceInfo) {
        resourceInfoDao.updateResourceInfo(resourceInfo);
    }

    @Override
    public ResourceInfo sava(ResourceInfo resourceInfo) {
        return resourceInfoDao.sava(resourceInfo);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByPid(String pid) {
        return resourceInfoDao.queryResourceInfoByPid(pid);
    }

    @Override
    public ResourceInfo queryResourceInfoById(ObjectId id) {
        return resourceInfoDao.queryResourceInfoById(id);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceName(String resourceName) {
        return resourceInfoDao.queryResourceInfoByResourceName(resourceName);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceUrl(String resourceUrl) {
        return resourceInfoDao.queryResourceInfoByResourceUrl(resourceUrl);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceAuth(String resourceAuth) {
        return resourceInfoDao.queryResourceInfoByResourceAuth(resourceAuth);
    }

    @Override
    public List<ResourceInfo> getResourceInfoByJson(String defaultString) {
        List<ResourceInfoDTO> resourceInfoDto = JSONObject.parseArray(defaultString, ResourceInfoDTO.class);

        //先检测ObjectId是否都有效
        for (ResourceInfoDTO infoDto : resourceInfoDto) {
            //ObjectId无效时
            if (!ObjectId.isValid(infoDto.getId())){
                //将无效的ObjectId进行替换
                ObjectId objectId = new ObjectId();
                //判断是否有子级
                if (!infoDto.getPid().equals("0") || !infoDto.getPid().equals("")){
                    for (ResourceInfoDTO resourceInfoDTO1 : resourceInfoDto) {
                        if (resourceInfoDTO1.getPid().equals(infoDto.getId())){
                            resourceInfoDTO1.setPid(String.valueOf(objectId));
                        }
                    }
                }

                infoDto.setId(String.valueOf(objectId));
            }
        }

        List<ResourceInfo> resourceInfo = new ArrayList<>();
        for (ResourceInfoDTO infoDto : resourceInfoDto) {
            ResourceInfo resourceInfo1 = new ResourceInfo();
            resourceInfo1.setId(new ObjectId(infoDto.getId()));
            resourceInfo1.setPid(infoDto.getPid());
            resourceInfo1.setIsView(infoDto.getIsView());
            resourceInfo1.setResourceAuth(infoDto.getResourceAuth());
            resourceInfo1.setResourceName(infoDto.getResourceName());
            resourceInfo1.setResourceUrl(infoDto.getResourceUrl());
            resourceInfo1.setStatus(infoDto.getStatus());
            resourceInfo1.setType(infoDto.getType());
            resourceInfo1.setCreateTime(infoDto.getCreateTime());
            resourceInfo1.setModifyTime(infoDto.getModifyTime());
            resourceInfo.add(resourceInfo1);
        }
        return resourceInfo;
    }

}
