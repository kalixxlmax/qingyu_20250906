package com.basic.im.admin.controller;

import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.user.entity.AllowRequestClient;
import com.basic.im.user.service.AllowRequestClientService;
import com.basic.im.vo.JSONMessage;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

/**
 * 授权IP 管理
 *
 * @author Administrator
 */
//@Api(value = "AllowRequestClientController", tags = "附近接口")
@ApiIgnore
@RestController
@RequestMapping(value = "/allowRequest", method = {RequestMethod.GET, RequestMethod.POST})
public class AllowRequestClientController {


    @Autowired
    private AllowRequestClientService allowRequestClientService;



    @RequestMapping(value = "/list")
    public JSONMessage queryList(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") Integer limit,
                                 @RequestParam(defaultValue = "") String keyword) {
        PageResult<AllowRequestClient> result =allowRequestClientService.queryList(page,
                limit, keyword);
        return JSONMessage.success(result);
    }


    @RequestMapping(value = "/update")
    public JSONMessage update(@ModelAttribute AllowRequestClient requestClient) {
        allowRequestClientService.updateAllowRequest(requestClient);
        return JSONMessage.success();
    }

    @RequestMapping(value = "/delete")
    public JSONMessage update(@RequestParam(defaultValue = "") String id) {
        if(!ObjectId.isValid(id)){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        allowRequestClientService.deleteAllowRequest(new ObjectId(id));
        return JSONMessage.success();
    }

    @RequestMapping(value = "/save")
    public JSONMessage save(AllowRequestClient allowRequestClient) {
        allowRequestClientService.updateAllowRequest(allowRequestClient);
        return JSONMessage.success();
    }


}
