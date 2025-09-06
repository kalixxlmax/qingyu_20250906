package com.basic.im.security.controller;

import cn.hutool.core.util.ObjectUtil;
import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.security.dto.WithdrawalDTO;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.ResultDataSelect;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.ResourceInfoManager;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.user.entity.Role;
import com.basic.im.user.service.impl.RoleManagerImpl;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtils;
import org.bson.types.ObjectId;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 管理后台  角色操作接口操作
 */
@ApiIgnore
@RestController
@RequestMapping("/console")
public class SecurityRoleController {

    @Autowired
    private SecurityRoleManager securityRoleManager;
    
    @Autowired
    private ResourceInfoManager resourceInfoManager;

    /**
     * @Description  添加角色
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/addSecurityRole")
    public JSONMessage addSecurityRole(SecurityRole securityRole){
        SecurityRole roleSecurity = securityRoleManager.querySecurityRoleByRoleName(securityRole.getRoleName());
        if (!ObjectUtil.isEmpty(roleSecurity) || securityRole.getRoleName().equals(KConstants.systemAccount.ADMIN_NAME)){
            return JSONMessage.failure("角色已存在！");
        }
        securityRole.setRole((byte) KConstants.EIGHT);
        securityRoleManager.sava(securityRole);
        return JSONMessage.success();
    }


    /**
     * @Description 删除角色
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/deleteSecurityRole")
    public JSONMessage deleteSecurityRole(String id){
        securityRoleManager.deleteRole(new ObjectId(id));
        return  JSONMessage.success();
    }



    /**
     * @Description 修改角色
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/updateSecurityRole")
    public JSONMessage updateSecurityRole(SecurityRole securityRole){
        securityRoleManager.updateRole(securityRole);
        return JSONMessage.success();
    }

    /**
     * @Description 修改角色资源权限
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/updateRoleResource")
    public JSONMessage updateRoleResource(String roleId,String auth){
        List<String> list = StringUtils.getListBySplit(auth, ",");
        securityRoleManager.updateRoleResource(new ObjectId(roleId),list);
        return JSONMessage.success();
    }

    /**
     * @Description 查询全部角色
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/querySecurityRole")
    public JSONMessage querySecurityRole(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit){
        PageResult<SecurityRole> data = securityRoleManager.querySecurityRole(page, limit);
        return JSONMessage.success(data);
    }

    /**
     * @Description 根据id查询角色
     * @Date 18:32 2020/3/6
     **/
    @RequestMapping(value = "/querySecirutyRoleById")
    public JSONMessage querySecirutyRoleById(String id){
        SecurityRole securityRole = securityRoleManager.querySecurityRoleById(new ObjectId(id));
        return JSONMessage.success(securityRole);
    }

    /**
     * @Description 查询全部角色
     * @Date 11:25 2020/3/6
     **/
    @RequestMapping(value = "/querySecurityRole1")
    public JSONMessage querySecurityRole1(){
        //全部角色
        List<SecurityRole> securityRoles = securityRoleManager.querySecurityRol1e();

        //替换实体
        List<ResultDataSelect> collect = securityRoles.stream().map(securityRole -> {
            ResultDataSelect resultDataSelect = new ResultDataSelect();
            resultDataSelect.setName(securityRole.getRoleName());
            resultDataSelect.setValue(String.valueOf(securityRole.getRoleId()));
            return resultDataSelect;
        }).collect(Collectors.toList());


        return JSONMessage.success(collect);
    }

    /**
     * 获取角色所有权限 和 资源
     * @param roleId
     * @return
     */
    @RequestMapping(value = "/queryRoleResourceInfo")
    public JSONMessage queryRoleResourceInfo(String roleId){
        SecurityRole securityRole = securityRoleManager.querySecurityRoleById(new ObjectId(roleId));
        //获取资源
        List<ResourceInfo> collect = securityRole.getRoleResourceList().stream().filter(resource -> !resource.equals(KConstants.STRING_NULL)).map(resource -> {
            ResourceInfo resourceInfo = resourceInfoManager.queryResourceInfoById(new ObjectId(resource));
            return resourceInfo;
        }).collect(Collectors.toList());
        return JSONMessage.success(collect);
    }

    /**
     * 获取角色所有资源
     * @param roleId
     * @return
     */
    @RequestMapping(value = "/queryRoleResourceId")
    public JSONMessage queryRoleResourceId(String roleId){
        SecurityRole securityRole = securityRoleManager.querySecurityRoleById(new ObjectId(roleId));
        List<String> collect = securityRole.getRoleResourceList().stream().filter(resource -> !resource.equals(KConstants.STRING_NULL)).collect(Collectors.toList());
        return JSONMessage.success(collect);
    }
    @Autowired
    private RedissonClient redissonClient;

    @RequestMapping(value = "/withdrawalConfig")
    public JSONMessage withdrawalConfig(String page, String limit){
        RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");


        List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
        //分页 list 模拟分页
        list = list.stream()
                .skip((Integer.parseInt(page) - 1) * Integer.parseInt(limit))
                .limit(Integer.parseInt(limit))
                .collect(Collectors.toList());;
        PageResult<WithdrawalDTO> data = new PageResult<>();
        data.setData(list);
        data.setTotal(withdrawalConfig.size());
        return JSONMessage.success(data);
    }


    @RequestMapping(value = "/withdrawalUpdateConfig")
    public JSONMessage withdrawalUpdateConfig(WithdrawalDTO param){
        RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");
        List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
        for (WithdrawalDTO d : list) {
            if(d.getId().equals(param.getId())){
                withdrawalConfig.remove(d);
                withdrawalConfig.add(param);
                break;
            }
        }

        return JSONMessage.success();
    }

    @RequestMapping(value = "/withdrawalDelConfig")
    public JSONMessage withdrawalDelConfig(WithdrawalDTO param){
        RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");
        List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
        for (WithdrawalDTO d : list) {
            if(d.getId().equals(param.getId())){
                withdrawalConfig.remove(d);
                break;
            }
        }

        return JSONMessage.success();
    }

    @RequestMapping(value = "/withdrawalAddConfig")
    public JSONMessage withdrawalAddConfig(WithdrawalDTO param){
        RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");
//        if(withdrawalConfig.size() > 0){
//            return JSONMessage.failure("只能添加一条配置");
//        }
        List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
        int i = list.stream().map(WithdrawalDTO::getId)
                .max(Integer::compareTo).orElse(0) + 1;
        param.setId(i);
        param.setType(i);
        withdrawalConfig.add(param);

        return JSONMessage.success();
    }
}
