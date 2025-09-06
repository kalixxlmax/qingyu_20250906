package com.basic.im.security.controller;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.ResultDataSelect;
import com.basic.im.security.entity.ResultDataSelectInfo;
import com.basic.im.security.service.Impl.SecurityRoleManagerImpl;
import com.basic.im.security.service.ResourceInfoManager;
import com.basic.im.user.service.impl.RoleManagerImpl;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 管理后台 资源接口操作
 **/
@ApiIgnore
@RestController
@RequestMapping("/console")
public class ResourceInfoController {

    @Autowired
    private ResourceInfoManager resourceInfoManager;

    @Autowired
    private SecurityRoleManagerImpl securityRoleManager;

    /**
     * 查询全部资源
     **/
    @RequestMapping(value = "/query/all/resourceinfo")
    public JSONMessage queryAllResourceInfo() {
        List<ResourceInfo> resultData = new ArrayList<>();
        List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfo();
        //一级菜单
        resourceInfos.stream().forEach(resourceInfo -> {
            if (StringUtil.isEmpty(resourceInfo.getPid())) {
                resultData.add(resourceInfo);
            }
        });

        //对时间升序
        List<ResourceInfo> resourceInfoDesc = resultData.stream().sorted(Comparator.comparing(ResourceInfo::getCreateTime)).collect(Collectors.toList());

        //二级菜单
        resourceInfoDesc.stream().forEach(resourceInfo -> {
            List<ResourceInfo> resourceInfos_ = resourceInfoManager.queryResourceInfoByPid(resourceInfo.getId().toString());
            //对时间升序
            List<ResourceInfo> resourceInfosDesc = resourceInfos_.stream().sorted(Comparator.comparing(ResourceInfo::getCreateTime)).collect(Collectors.toList());
            resourceInfo.setChildren(resourceInfosDesc);
        });

        return JSONMessage.success(resourceInfoDesc);
    }


    /**
     * 删除资源
     **/
    @RequestMapping(value = "/delResourceInfo")
    public JSONMessage delResourceInfo(String id) {
        //是否有上级资源
        List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfoByPid(id);
        ObjectId objectId = new ObjectId();

        if (resourceInfos.size() <= KConstants.ZERO){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.RESOURCE_EXIST_DATA);
        }

        //目前只有一个元素（展示数据）
        if (resourceInfos.size() == KConstants.ONE && resourceInfos.get(0).getIsView() == KConstants.ZERO) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.RESOURCE_EXIST_DATA);
        }

        if (resourceInfos.size() == KConstants.ONE) {
            objectId = resourceInfos.get(0).getId();
        }

        //维护角色权限数据
        securityRoleManager.maintainSecurityRole(new ObjectId(id));
        //维护用户权限资源数据
        securityRoleManager.maintainRole(new ObjectId(id));
        //删除资源
        resourceInfoManager.delResourceInfo(objectId);
        resourceInfoManager.delResourceInfo(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 修改资源
     **/
    @RequestMapping(value = "/updateResourceInfo")
    public JSONMessage updateResourceInfo(ResourceInfo resourceInfo, String resourceId) {
        resourceInfo.setId(new ObjectId(resourceId));
        resourceInfoManager.updateResourceInfo(resourceInfo);
        return JSONMessage.success();
    }

    /**
     * 添加资源
     **/
    @RequestMapping(value = "/addResourceInfo")
    public JSONMessage sava(ResourceInfo resourceInfo) {
        if (ObjectUtil.isEmpty(resourceInfo.getResourceAuth())) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.RESOURCE_AUTH_NOTNULL);
        }
        List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfoByResourceAuth(resourceInfo.getResourceAuth());
        if (resourceInfos.size() > KConstants.ZERO) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.RESOURCE_AUTH_EXIST_DATA);
        }

        //判断是否是菜单
        ResourceInfo newResourceInfo = resourceInfoManager.sava(resourceInfo);
        //菜单加入子菜单  【展示数据】
        if (newResourceInfo.getType().equals(KConstants.PAGE_ONE)) {
            ResourceInfo resource = new ResourceInfo();
            resource.setType(KConstants.PAGE_ONE);
            resource.setPid(String.valueOf(newResourceInfo.getId()));
            resource.setStatus((byte) KConstants.ONE);
            resource.setIsView(KConstants.ONE);
            resource.setResourceUrl(KConstants.PAGE_ONE);
            resource.setResourceName("展示数据");
            resource.setResourceUrl(KConstants.STRING_NULL);
            resourceInfoManager.sava(resource);
        }

        return JSONMessage.success();
    }

    /**
     * 查询资源根据id
     **/
    @RequestMapping(value = "/queryResourceInfoById")
    public JSONMessage sava(String id) {
        ResourceInfo data = resourceInfoManager.queryResourceInfoById(new ObjectId(id));
        return JSONMessage.success(data);
    }

    /**
     * 查询资源
     * 用户多选下拉菜单
     **/
    @RequestMapping(value = "/querySelectResource")
    public JSONMessage querySelectResource() {
        List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfo();
        //替换实体
        List<ResultDataSelect> collect = resourceInfos.stream().map(resourceInfo -> {
            ResultDataSelect resultDataSelect = new ResultDataSelect();
            resultDataSelect.setName(resourceInfo.getResourceName());
            resultDataSelect.setValue(String.valueOf(resourceInfo.getId()));
            return resultDataSelect;
        }).collect(Collectors.toList());

        return JSONMessage.success(collect);
    }

    /**
     * 查看全部资源
     **/
    @RequestMapping(value = "/querySelectAllResource")
    public JSONMessage querySelectAllResource() {
        //查出全部资源
        List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfo();
        List<ResultDataSelectInfo> data = new ArrayList<>();

        //先将最高的父节点给遍历出来
        for (ResourceInfo resourceInfo : resourceInfos) {
            if (resourceInfo.getPid().equals("0") || resourceInfo.getPid().equals("")) {
                if (resourceInfo.getStatus() == 1) {
                    //判读是否禁用
                    ResultDataSelectInfo resultDataSelectInfo = new ResultDataSelectInfo();
                    resultDataSelectInfo.setId(String.valueOf(resourceInfo.getId()));
                    resultDataSelectInfo.setResourceName(resourceInfo.getResourceName());
                    resultDataSelectInfo.setResourceAuth(resourceInfo.getResourceAuth());
                    resultDataSelectInfo.setResourceUrl(resourceInfo.getResourceUrl());
                    resultDataSelectInfo.setIsView(resourceInfo.getIsView());
                    resultDataSelectInfo.setStatus(resourceInfo.getStatus());
                    resultDataSelectInfo.setType(resourceInfo.getType());
                    data.add(resultDataSelectInfo);
                }
            }
        }

        //遍历刚刚数据的最高节点

        data.forEach(datum->{
            //子节点数据
            List<ResourceInfo> resourceInfoList = resourceInfoManager.queryResourceInfoByPid(String.valueOf(datum.getId()));
            List<ResultDataSelectInfo> list = new ArrayList<>();
            if (resourceInfoList.size() <= KConstants.ZERO) {
                return;
            }
            //将遍历出来的二级节点加入
            for (ResourceInfo resourceInfo : resourceInfoList) {
                if (resourceInfo.getStatus() == KConstants.ONE) {
                    ResultDataSelectInfo resultDataSelect = new ResultDataSelectInfo();
                    resultDataSelect.setResourceName(resourceInfo.getResourceName());
                    resultDataSelect.setResourceAuth(resourceInfo.getResourceAuth());
                    resultDataSelect.setResourceUrl(resourceInfo.getResourceUrl());
                    resultDataSelect.setStatus(resourceInfo.getStatus());
                    resultDataSelect.setIsView(resourceInfo.getIsView());
                    resultDataSelect.setType(resourceInfo.getType());
                    resultDataSelect.setId(String.valueOf(resourceInfo.getId()));
                    List<ResultDataSelectInfo> recursion = recursion(resourceInfo);
                    resultDataSelect.setChildren(recursion);
                    list.add(resultDataSelect);
                }
            }
            datum.setChildren(list);
        });

        return JSONMessage.success(data);
    }

    /**
     * 递归查询子菜单
     * @param resourceInfo
     * @return
     */
    private List<ResultDataSelectInfo> recursion(ResourceInfo resourceInfo) {
        List<ResultDataSelectInfo> resultDataSelect = new ArrayList<>();
        List<ResourceInfo> resourceInfosPid = resourceInfoManager.queryResourceInfoByPid(String.valueOf(resourceInfo.getId()));
        if (resourceInfosPid.size() <= KConstants.ZERO){
            return resultDataSelect;
        }

        resourceInfosPid.stream()
                .filter(resource -> resource.getStatus() == KConstants.ONE)
                .map(resource -> {
                    ResultDataSelectInfo resultDataSelectTwo = new ResultDataSelectInfo();
                    resultDataSelectTwo.setId(String.valueOf(resource.getId()));
                    resultDataSelectTwo.setResourceName(resource.getResourceName());
                    resultDataSelectTwo.setResourceAuth(resource.getResourceAuth());
                    resultDataSelectTwo.setResourceUrl(resource.getResourceUrl());
                    resultDataSelectTwo.setIsView(resource.getIsView());
                    resultDataSelectTwo.setStatus(resource.getStatus());
                    resultDataSelectTwo.setType(resource.getType());
                    //获取子菜单
                    List<ResultDataSelectInfo> recursion = recursion(resource);
                    resultDataSelectTwo.setChildren(recursion);
                    return resultDataSelectTwo;
                }).collect(Collectors.toList());
        return resultDataSelect;
    }


}
