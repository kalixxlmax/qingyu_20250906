package com.basic.im.admin.controller;

import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.qrcode.QrCodeUtil;
import com.basic.im.user.dao.InviteCodeDao;
import com.basic.im.user.entity.InviteCode;
import com.basic.im.utils.GoogleAuthenticator;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.User;
import com.basic.im.vo.JSONMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;
import sun.misc.BASE64Encoder;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@ApiIgnore
@RestController
@RequestMapping(value = "/console")
public class AdminAuthenticatorController {

    @Autowired
    private UserDao userDao;

    @Autowired
    protected InviteCodeDao inviteCodeDao;

    @GetMapping("getSecret")
    public String getSecret() {
        return GoogleAuthenticator.getSecretKey();
    }

    @GetMapping("saveSecretKey")
    public JSONMessage saveSecretKey(String secretKey,int userId){
        Map<String,Object> map = new HashMap<>();
        map.put("secretKey",secretKey);
        userDao.updateUser(userId,map);
        return JSONMessage.success();
    }

    /**
     * 生成二维码，APP直接扫描绑定，两种方式任选一种
     */
    @GetMapping("getQrcode")
    public JSONMessage getQrcode(String name, HttpServletResponse response) throws Exception {
        String secretKey = GoogleAuthenticator.getSecretKey();
        // 生成二维码内容
        String qrCodeText = GoogleAuthenticator.getQrCodeText(secretKey, name, "");
//
        BufferedImage bufferedImage = QrCodeUtil.generate(qrCodeText, 300, 300);
        Map<String,Object> result = new HashMap<>();
        result.put("url",bufferedImageToBase64(bufferedImage));
        result.put("secretKey",secretKey);


        return JSONMessage.success(result);

        // 生成二维码输出
       // new SimpleQrcodeGenerator().generate(qrCodeText).toStream(response.getOutputStream());
    }

    @GetMapping("getQrcodeV2")
    public JSONMessage getQrcodeV2(int userId){
        User user = userDao.getUser(userId);
        if(user == null){
            return JSONMessage.failure("账号不存在");
        }
        if(StrUtil.isBlank(user.getSecretKey())){
            return JSONMessage.failure("未绑定谷歌验证器");
        }

        String qrCodeText = GoogleAuthenticator.getQrCodeText(user.getSecretKey(), user.getUserId()+"", "");
//
        BufferedImage bufferedImage = QrCodeUtil.generate(qrCodeText, 300, 300);
        Map<String,Object> result = new HashMap<>();
        result.put("url",bufferedImageToBase64(bufferedImage));
        return JSONMessage.success(result);

    }

    private  String bufferedImageToBase64(BufferedImage bufferedImage) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();//io流
        try {
            ImageIO.write(bufferedImage, "png", baos);//写入流中
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = baos.toByteArray();//转换成字节
        BASE64Encoder encoder = new BASE64Encoder();
        String png_base64 = encoder.encodeBuffer(bytes).trim();//转换成base64串
        png_base64 = png_base64.replaceAll("\n", "").replaceAll("\r", "");//删除 \r\n
        System.out.println("值为：" + "data:image/jpg;base64," + png_base64);
        return "data:image/jpg;base64," + png_base64;
    }

    /**
     * 获取code
     */
    @GetMapping("getCode")
    public String getCode(String secretKey) {
        return GoogleAuthenticator.getCode(secretKey);
    }

    /**
     * 验证 code 是否正确
     */
    @GetMapping("checkCode")
    public JSONMessage checkCode(int userId, String code) {
        User user = userDao.getUser(userId);
        if(user == null){
            return JSONMessage.failure("账号不存在");
        }
        if(StrUtil.isBlank(user.getSecretKey())){
            return JSONMessage.failure("未绑定谷歌验证器");
        }

        boolean b = GoogleAuthenticator.checkCode(user.getSecretKey(), Long.parseLong(code), System.currentTimeMillis());
        if (b) {
            return JSONMessage.success();
        }
        return JSONMessage.failure("验证码错误");
    }

    @PostMapping("updateInviteCode")
    public JSONMessage updateInviteCode(int userId, String code){
        User user = userDao.getUser(userId);
        if(user == null){
            return JSONMessage.failure("账号不存在");
        }

        InviteCode inviteCodeByCode = inviteCodeDao.findInviteCodeByCode(code);
        if(inviteCodeByCode != null){
            return JSONMessage.failure("该邀请码已存在");
        }

        inviteCodeDao.updateUserInviteCode(userId,code);
        return JSONMessage.success();
    }

}
