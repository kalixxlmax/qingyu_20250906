package com.basic.im.api.controller;
import com.basic.im.api.service.base.AbstractController;
import com.basic.im.msg.service.impl.FxManagerImpl;
import com.basic.im.vo.JSONMessage;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 音乐模块接口
 *
 * @author Administrator
 */

@RestController
@RequestMapping(value = "/fx", method = {RequestMethod.GET, RequestMethod.POST})
public class FxController extends AbstractController {

    @Autowired
    private FxManagerImpl fxManager;

    @ApiOperation("查询音乐列表")
    @RequestMapping(value = "/list")
    public JSONMessage queryMusicList(@RequestParam(defaultValue = "0") int pageIndex,
                                      @RequestParam(defaultValue = "20") Integer pageSize) {
        Object data = fxManager.queryMusicInfo(pageIndex, pageSize);
        return JSONMessage.success(data);
    }

}
