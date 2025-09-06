package com.basic.im.appleLogin;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.auth0.jwk.Jwk;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.HttpUtil;
import io.jsonwebtoken.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.stereotype.Component;

import java.security.PublicKey;
import java.util.HashMap;

/**
 * @author wcl
 * @version V1.0
 * @Description: Apple登录，苹果服务器交互和验证SignInWithApple
 * @date 2020/7/9 16:26
 */
@Component
@Slf4j
public class SignInWithApple {

    private final static String AUTH_KEYS = "https://appleid.apple.com/auth/keys";
    private final static String APPLE_HOST = "https://appleid.apple.com";

    private static String SUCCESS = "SUCCESS";
    private static String FAIL = "FAIL";

    /**
     * 生成公钥串
     *
     * @return 构造好的公钥串
     */
    private static JSONArray getPublicKey() {
        try {
            String result = HttpUtil.URLGet(AUTH_KEYS, new HashMap<>());
            JSONObject data = JSONObject.parseObject(result);
            String keys = data.getString("keys");
            return JSONObject.parseArray(keys);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new ServiceException(KConstants.ResultCode.APPLELOG_INERROR);
        }
    }

    /**
     * 解密信息
     *
     * @param identityToken APP获取的identityToken
     * @return 解密参数：失败返回null
     */
    public static String verify(String identityToken) {
        try {
            if (identityToken.split("\\.").length > 1) {
                String claim = new String(Base64.decodeBase64(identityToken.split("\\.")[1]));
                String aud = JSONObject.parseObject(claim).get("aud").toString();
                String sub = JSONObject.parseObject(claim).get("sub").toString();
                String reuslt = FAIL;
                //此处循环遍历的原因是因为只取第一个会出现非法token的异常
                for (int i = 0; i < getPublicKey().size(); i++) {
                    JSONObject publicKeys = JSONObject.parseObject(getPublicKey().getString(i));
                    Jwk jwa = Jwk.fromValues(publicKeys);
                    PublicKey publicKey = jwa.getPublicKey();
                    try {
                        reuslt = verify(publicKey, identityToken, aud, sub);
                    } catch (Exception e) {
                        if (i == 0) {
                            continue;
                        }
                    }
                    if (reuslt.equals(SUCCESS)) {
                        break;
                    }
                }
                if (reuslt.equals(SUCCESS)) {
                    return claim;
                } else {
                    return FAIL;
                }
            }
        } catch (Exception e) {
            throw new ServiceException(KConstants.ResultCode.APPLELOG_INERROR);
        }
        return null;
    }

    /**
     * 此处验证
     *
     * @param key
     * @param jwt
     * @param audience
     * @param subject
     * @return
     * @throws Exception
     */
    public static String verify(PublicKey key, String jwt, String audience, String subject) throws Exception {
        String result = FAIL;
        JwtParser jwtParser = Jwts.parser().setSigningKey(key);
        jwtParser.requireIssuer(APPLE_HOST);
        jwtParser.requireAudience(audience);
        jwtParser.requireSubject(subject);
        try {
            Jws<Claims> claim = jwtParser.parseClaimsJws(jwt);
            if (claim != null && claim.getBody().containsKey("auth_time")) {
                result = SUCCESS;
                return result;
            }
        } catch (ExpiredJwtException e) {
            log.error("苹果token过期", e);
            throw new ServiceException(KConstants.ResultCode.APPLELOGIN_INFORMATION_EXPIRED);
        } catch (SignatureException e) {
            log.error("苹果token非法", e);
            throw new ServiceException(KConstants.ResultCode.APPLELOG_INERROR);
        }
        return result;
    }

    /**
     * String identityToken, 拉取信息
     * String appleLoginCode, 苹果获取的用户唯一标识
     * Apple验证
     * @param
     * @return
     */
    public static String appleLogin(String identityToken, String appleLoginCode) {
        // 请求接口获取返回值
        String result;
        try {
            result = SignInWithApple.verify(identityToken);
        } catch (ServiceException e) {
            throw new ServiceException(e.getResultCode());
        }
        //验证成功后
        if (!result.equals("FAIL")) {// 请求成功
            JSONObject appleResult = JSONObject.parseObject(result);
            if (!appleResult.getString("sub").equals(appleLoginCode)) {
                log.error("appleLogin subCode client: {},server: {}", appleLoginCode, appleResult.getString("sub"));
                throw new ServiceException(KConstants.ResultCode.APPLELOG_INERROR);
            }
            log.info("SignInWithApple.verify ：{}", result);
            return appleLoginCode;
        } else {
            //验证失败
            throw new ServiceException(KConstants.ResultCode.APPLELOG_INERROR);
        }
    }
}
