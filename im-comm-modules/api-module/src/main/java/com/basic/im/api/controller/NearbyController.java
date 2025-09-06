package com.basic.im.api.controller;

import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.config.AppConfig;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.room.vo.NearByRoom;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.NearbyUser;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.utils.MongoUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * 附近接口
 *
 * @author Administrator
 */
@Api(value = "NearbyController", tags = "附近接口")
@RestController
@RequestMapping(value = "/nearby", method = {RequestMethod.GET, RequestMethod.POST})
public class NearbyController {

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private RoomManagerImplForIM roomManager;

    @ApiOperation("附近的用户")
    @RequestMapping(value = "/user")
    public JSONMessage nearbyUser(@ModelAttribute NearbyUser poi) {
        try {
            if (StringUtil.isEmpty(poi.getNickname())) {
                byte disableNearbyUser = appConfig.getDisableNearbyUser();
                if (1 == disableNearbyUser) {
                    // 内部版本专用配置,严禁修改
                    if (0 == poi.getPageIndex() && !StringUtil.isEmpty(SKBeanUtils.getSystemConfig().getDefaultTelephones())) {
                        String[] split = SKBeanUtils.getSystemConfig().getDefaultTelephones().split(",");
                        List<User> dataList = new ArrayList<>();
                        User user;
                        for (String phone : split) {
                            user = userManager.getUser(phone);
                            if (null != user) {
                                user.buildNoSelfUserVo(ReqUtil.getUserId());
                                user.setLoc(new User.Loc(poi.getLongitude(), poi.getLatitude()));
                                dataList.add(user);
                            }
                        }
                        return JSONMessage.success(dataList);

                    } else {
                        return JSONMessage.success(new ArrayList<>());
                    }
                }
            }
            poi.setNickname(MongoUtil.tranKeyWord(poi.getNickname()));
            List<User> nearbyUser = userManager.nearbyUser(poi, VerifyUtil.verifyUserId(ReqUtil.getUserId()));
            return JSONMessage.success(nearbyUser);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }


    @ApiOperation("附近的用户")
    @RequestMapping(value = "/nearbyUserWeb")
    public JSONMessage nearbyUserWeb(@ModelAttribute NearbyUser poi) {
        try {
            Object nearbyUser = userManager.nearbyUserWeb(poi);
            return JSONMessage.success(nearbyUser);
        } catch (Exception e) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.UserNotExist);
        }

    }


    @RequestMapping("/newUser")
    @ApiOperation("最新的用户")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "access_token", value = "授权钥匙", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页码数", dataType = "String"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据条数", dataType = "String")
    })
    public JSONMessage newUser(@RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "12") int pageSize, @RequestParam(defaultValue = "0") int isAuth) {
        JSONMessage jMessage = null;
        try {
            String phone = userManager.getUser(ReqUtil.getUserId()).getTelephone();
            // 内部版本专用配置,严禁修改
            if (!SKBeanUtils.getSystemConfig().getDefaultTelephones().contains(phone)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
            }
            List<User> dataList = userManager.getUserlimit(pageIndex, pageSize, isAuth);
            if (null != dataList && dataList.size() > 0) {
                User.LoginLog loginLog;
                for (User user : dataList) {
                    loginLog = userManager.getLogin(user.getUserId());
                    user.setLoginLog(loginLog);
                }
                jMessage = JSONMessage.success(null, dataList);
            }
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }

        return jMessage;
    }

    @ApiOperation("附近的群组")
    @RequestMapping(value = "/nearbyRoom")
    public JSONMessage nearbyRoom(@ModelAttribute NearByRoom nearByRoom) {
        try {
            nearByRoom.setUserId(ReqUtil.getUserId());
            return JSONMessage.success(roomManager.nearbyRoom(nearByRoom));
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }
}
