package com.basic.sysapi;

/**
 * RequestApiEnum <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/9  <br>
 */
public interface SystemApiEnum {

    /**
     * 获取系统配置
     */
    String CONFIG="/config";

    /**
     * 获取系统操作 token
     */
    String GET_ACCESS_TOKEN="/sysapi/getAccess_token";


    /**
     * 同步注册用户
     */
    String SYNC_REGISTER_USER="/sysapi/syncRegisterUser";

    /**
     * 同步修改用户信息
     */
    String SYNC_USERINFO="/sysapi/syncUserInfo";

    /**
     * 根据第三方 ThirdId 查询用户信息
     */
    String QUERY_USERINFO="/sysapi/queryUserInfo";


    /**
     * 同步用户登陆 token 接口调用凭证
     */
    String SYNC_LOGIN_SESSION="/sysapi/syncLoginSession";

    /**
     * 清除用户登陆 Session
     */
    String CLEAR_LOGIN_SESSION="/sysapi/clearLoginSession";

}
