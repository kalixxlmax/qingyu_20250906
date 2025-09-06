package com.basic.im.user.service.impl;


import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.friends.dao.FriendsDao;
import com.basic.im.friends.service.FriendsRedisRepository;
import com.basic.im.room.dao.RoomDao;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.RoleDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.SKBeanUtils;
import com.google.common.reflect.ClassPath;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Slf4j
@Service
public class RoleManagerImpl{

	@Autowired
	private RoleDao roleDao;
	public RoleDao getRoleDao(){
		return roleDao;
	}
	@Autowired
	private UserDao userDao;

	@Lazy
	@Autowired
	private RoomDao roomDao;

	@Lazy
	@Autowired
	private FriendsDao friendsDao;

	@Autowired
	private FriendsRedisRepository firendsRedisRepository;

	@Autowired
	private UserCoreRedisRepository userCoreRedisRepository;

	@Autowired
	private UserManagerImpl userManager;


	public int getUserRoleByUserId(Integer userId){
		Object roleQuery = getRoleDao().queryOneField("role", new Document("userId", userId));
		return null == roleQuery ? 0 : (int) roleQuery;
	}

	public Role getUserRole(Integer userId,String phone,Integer type){
		Role role = getRoleDao().getUserRole(userId,phone,type);
		return role;
	}

	public List<Role> getUserRoles(Integer userId,String phone,Integer type){
		return getRoleDao().getUserRoleList(userId,phone,type);
	}

	public List<Integer> getUserRoles(Integer userId){
		List<Integer> roleType = new ArrayList<Integer>();
		List<Role> asList = getRoleDao().getUserRoleList(userId,null,null);
		asList.forEach(role -> {
			roleType.add((int) role.getRole());
		});
		return roleType;
	}

	// 后台管理管理员模块
	public PageResult<Role> adminList(String keyWorld, int page, int limit, Integer type, Integer userId ,String roleId){
		PageResult<Role> result = new PageResult<Role>();

		result = getRoleDao().getAdminRoleList(keyWorld,page,limit,type,userId,roleId);
		result.getData().forEach(role ->{
			role.setNickName(userManager.getNickName(role.getUserId()));
		});

		return result;
	}

	public void addRole(Role role){
		roleDao.addRole(role);
	}


	/**
	 * @Description （申请通过的公众号角色）
	 * @Date 13:00 2020/3/17
	 **/
	public void addOff(Integer userId,String telePhone, String phone, byte role, Integer type) {
		User accountUser = userManager.getUser(telePhone);
		if (null == accountUser) {
            throw new ServiceException("用户不存在");
        }

		Role userRole = getUserRole(accountUser.getUserId(), phone,null);
		if(null != userRole){
			throw new ServiceException("该账号已经是" + (userRole.getRole() == 5 ? "管理员" : userRole.getRole() == 6 ? "系统管理员" : userRole.getRole() == 1 ? "游客" :
					userRole.getRole() == 4 ? "客服" : userRole.getRole() == 2 ? "公众号" : userRole.getRole() == 3 ? "机器人" : "有其他身份"));
		}

		Role accountRole = null;
		if(type == 4) {
            accountRole = new Role(userId, accountUser.getTelephone(), role, (byte) 1, 0,promotionUrl(accountUser.getUserId()));
        } else {
            accountRole = new Role(userId, accountUser.getTelephone(), role, (byte) 1, 0);
        }
		getRoleDao().addRole(accountRole);

		updateFriend(accountUser.getUserId(), 2);
	}


	/** @Description:（设置管理员）
	* @param role
	**/
	public void addAdmin(String telePhone, byte role, Integer type,String roleId,List<String> roleResourceList) {
		User accountUser = userManager.getUser(telePhone);
		if (null == accountUser) {
            throw new ServiceException("用户不存在");
        }

		Role userRole = getUserRole(accountUser.getUserId(), accountUser.getPhone(),null);
		if(null != userRole){
			throw new ServiceException("该账号已经是" + (userRole.getRole() == 5 ? "管理员" : userRole.getRole() == 6 ? "系统管理员" : userRole.getRole() == 1 ? "游客" :
				userRole.getRole() == 4 ? "客服" : userRole.getRole() == 2 ? "公众号" : userRole.getRole() == 3 ? "机器人" : "有其他身份"));
		}
		/*if(null != userRole){
			byte roles = userRole.getRole();
			if(0 == type){
				if (userRole.getRole() == 5 || userRole.getRole() == 6 || userRole.getRole() == 1)
					throw new ServiceException("该账号已经是" + (userRole.getRole() == 5 ? "管理员" : userRole.getRole() == 6 ? "系统管理员" : "游客" ));
			}else if(4 == type){
				if (userRole.getRole() == 4)
					throw new ServiceException("该账号已经是客服人员");
			}else if(7 == type){
				if (userRole != null && userRole.getRole() == 7)
					throw new ServiceException("该账号已经是财务人员");
			}else if(1 == type){
				if (userRole != null && userRole.getRole() == 1)
					throw new ServiceException("该账号已经是游客");
				else if(userRole.getRole() == 5 || userRole.getRole() == 6)
					throw new ServiceException("该账号已经是"+(userRole.getRole() == 5 ? "管理员" : "超级管理员"));
			}
		}*/
		Role accountRole = null;
		if(type == 4) {
            accountRole = new Role(accountUser.getUserId(), accountUser.getTelephone(), role, (byte) 1, 0,promotionUrl(accountUser.getUserId()));
        } else {
            accountRole = new Role(accountUser.getUserId(), accountUser.getPhone(), role, (byte) 1, 0);
        }
		//设置角色编号
		accountRole.setRoleId(roleId);
		accountRole.setResourceIdList(roleResourceList);
		getRoleDao().addRole(accountRole);
//		updateFriend(accountUser.getUserId(),null);
	}

	public void delAdminById(String adminId,Integer type,Integer adminUserId) {
		if(type == 3){
			ThreadUtils.executeInThread(new Callback() {

				@Override
				public void execute(Object obj) {
					if(!StringUtil.isEmpty(adminId)){
						String[] admins = StringUtil.getStringList(adminId,",");
						userManager.deleteUser(adminUserId,admins);
						for (String userId : admins) {
							getRoleDao().deleteAdminRole(Integer.valueOf(userId),type);
						}
					}
				}
			});
		}else if(type == 2){
			Map<String,Object> map = new HashMap<>(1);
			map.put("userType",0);
			userDao.updateUser(Integer.parseInt(adminId),map);
			getRoleDao().deleteAdminRole(Integer.valueOf(adminId),type);
			updateFriend(Integer.valueOf(adminId),0);
			// 删除redis中的用户
			userCoreRedisRepository.deleteUserByUserId(Integer.valueOf(adminId));
		}else{
			getRoleDao().deleteAdminRole(Integer.valueOf(adminId),type);
			updateFriend(Integer.valueOf(adminId),0);
		}
			
	}
	
	
	public Role modifyRole(Role role){
		
		Map<String,Object> map = new HashMap<>(6);
		if(role.getRole() != 0) {
			map.put("role", role.getRole());
		}
		
		if(role.getStatus() != 0) {
			map.put("status", role.getStatus());
		}
		
		if(0 != role.getLastLoginTime()) {
            map.put("lastLoginTime", role.getLastLoginTime());
        }

		if(!StringUtil.isEmpty(role.getPromotionUrl())){
			map.put("promotionUrl", role.getPromotionUrl());
			// 维护群组的推广链接
			Map<String,Object> roomOps = new HashMap<>(1);
			roomOps.put("promotionUrl", role.getPromotionUrl());
			roomDao.updateRoomByUserId(role.getUserId(),roomOps);
		}
		Role findAndModify = getRoleDao().updateRole(role.getUserId(),role.getRole(),map);
		updateFriend(role.getUserId(),null);
		if(role.getStatus()==-1){
			//维护redis中的数据
			KSessionUtil.removeAdminToken(role.getUserId());
			userCoreRedisRepository.deleteUserByUserId(role.getUserId());
		}

		return findAndModify;
	}
	
	private String promotionUrl(Integer userId){
		/**
		 * 示例：http://www.duoyewu.com/tn/?pid=10000&com=2
			pid后面的数字是你的推广ID,该ID很重要。
			com后面的数字有3个参数：
			1. 直接跳转到招商页面
			2. 直接跳转到首页
			3 .直接跳转到注册页面
		 */
		String promotionUrl = SKBeanUtils.getSystemConfig().getPromotionUrl();
		if(StringUtil.isEmpty(promotionUrl)) {
            throw new ServiceException("请先在后台管理中设置在线咨询链接");
        }
		return new StringBuffer().append(promotionUrl).append(userId).toString();
	}
	
	// 修改好友关系表中的toUserType,toUserType中只维护0和2;
	public void updateFriend(Integer toUserId,Integer userType){
		List<Integer> roles = getUserRoles(toUserId);
		Map<String,Object> map = new HashMap<>(2);
		map.put("toFriendsRole", roles);
		if(null != userType){
			if(0 == userType){
				Role role = roleDao.getUserRoleByUserId(toUserId);
				if(null != role) {
                    roleDao.deleteRole(toUserId);
                }
			}
			map.put("toUserType", userType);
		}
//		friendsDao.updateFriends(toUserId,0,map);
		friendsDao.updateFriends(0,toUserId,map);
		ThreadUtils.executeInThread((Callback) obj -> {
			List<Integer> queryFansIdByUserId = friendsDao.queryFriendUserIdList(toUserId);
			//log.info("updateFriend === userId "+JSONObject.toJSONString(queryFansIdByUserId));
			queryFansIdByUserId.forEach(userId ->{
				firendsRedisRepository.deleteFriends(userId);
			});
		});
	}

	public void deleteAllRoles(Integer userId){
		getRoleDao().deleteRole(userId);
	}

	//根据用户编号获取信息
	 public Role getUserRole(Integer userId){
		return getRoleDao().getRoleByUserId(userId);
	}

	//修改用户角色
	public void updateUserRole(ObjectId id, String roleId, String roleName){
		getRoleDao().updateUserRole(id,roleId,roleName);
	}

	//修改用户资源
	public void updateUserRole(Integer id,List<String> resourceIdList){
		getRoleDao().updateUserRole(id,resourceIdList);
	}

	//查询全部资源
	public List<Role> queryAllRole(){
		return getRoleDao().queryAllRole();
	}

	/**
	 * @Description 根据编号删除该用户角色
	 * @Date 14:53 2020/3/20
	 **/
	public void deleteUserByRole(String id){
		roleDao.deleteById(new ObjectId(id));
	}

	//修改用户角色编号
	public void updateUserRole( int userId,String roleId){
		getRoleDao().updateUserRole(userId,roleId);
	}

	public Role getRole(Integer userId){
		Role role = getRoleDao().getUserRoleByUserId(userId);
		return role;
	}

}
