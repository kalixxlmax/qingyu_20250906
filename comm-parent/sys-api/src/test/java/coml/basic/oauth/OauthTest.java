package coml.basic.oauth;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.jwt.JWT;
import cn.hutool.jwt.JWTPayload;
import cn.hutool.jwt.JWTUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.sysapi.SystemRequestClient;
import com.basic.sysapi.model.SyncUserInfoModel;
import com.basic.utils.Base64;
import com.basic.utils.Md5Util;
import com.basic.utils.SnowflakeUtils;
import com.basic.utils.StringUtil;
import com.basic.utils.encrypt.AES;
import com.basic.utils.encrypt.RSA;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * OauthTest <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/9  <br>
 */
public class OauthTest {

    private static String privateKey="MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAJ0Q3g3JCj3akMHzlyWBmOEIwE/EfWKOFuzXPiAfWHnnyI/CTkIBx5XkX8HOtdqSGeX94FctlIFdhbbqFsolqDZ08NtfhKar3tbWsSlYrLzX25DKwlfcVOc2YIMcWpZgxzC8jkMCo1ch/ryOmpKvz08XLxpYFcPC2fSObbWefpz9AgMBAAECgYBe9UGqx+7fMnLuL50j2dRZyParwzmjChCmJAO1W/4bxZwl/e1eTsEhyC50d8rOWCI8QF8PlckA5U/gJGqe0GF/gcWSn9xXWpNrDzvDtXK6Y6GwsnwOcjw/Ot2hNf3badXq9R0o8F3Ct/FqgdBASj2KkqF9UKFLKSfIsNMnvy0oQQJBAP2rXI7EFVu/9K2ky0nlkHPIjZ+mllTNTdFMSzNTvR6KmpZYz9xMBLhAhnGnEeqPvnolSOEYUwkm2FMenkH10sUCQQCegkqWgYnjxtJTmHMwwqbWqsR+uPkrjvsupT4nclNjOyxCP7+WToPUQlZLTEOr7AtE3tBTrs+E5M9KGxN4QmTZAkAeodTZgKA7piB734yU7d3VvYAsqUc6Eli1T4s9NX1+9KnaQftH1P406cXSb6RgON99jIcSd4d/cWtqDiZ6PJ8pAkEAnTcJl+H/zCXlcvigN9q48+4IWtBIg4WbaRaIYUOppaCJM2RbOE/DvYHWaXTJIfpK6xI8euPF/D+dwhi85OilcQJATSADwMtM1SFTSbFreFfdHd5fvcksyuEilWXQ5I8l2qpDhYricpSXxRz2YHX/GX1QwtXp6p6+b8/RdFn9JtocXg==";

    private static String publicKey="MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCdEN4NyQo92pDB85clgZjhCMBPxH1ijhbs1z4gH1h558iPwk5CAceV5F/BzrXakhnl/eBXLZSBXYW26hbKJag2dPDbX4Smq97W1rEpWKy819uQysJX3FTnNmCDHFqWYMcwvI5DAqNXIf68jpqSr89PFy8aWBXDwtn0jm21nn6c/QIDAQAB";

    private static String apiSecret="1a316569125a43bcabea0bc9c3751f7a";

    private static String apiUrl="http://192.168.0.168:8092";

    private static String token= "8e905050ac8d4ec8a08a79107b419fc9";
    @Test
    public void rsaRequestTest() {
        String data="123456";
        byte[] encrypt = RSA.encrypt(data.getBytes(StandardCharsets.UTF_8), Base64.decode(publicKey));
        String encodeBase64 = Base64.encode(encrypt);
        System.out.println(encodeBase64);
        System.out.println(new String(RSA.decrypt(Base64.decode(encodeBase64),Base64.decode(privateKey))));

    }

    @Test
    public void syncRegisterUserTest() {
        try {
            SystemRequestClient requestClient=new SystemRequestClient(apiUrl,publicKey,apiSecret);
            requestClient.setAccess_token(token);
            //requestClient.requestAccess_token();
            System.out.println("token  "+requestClient.getAccess_token());


            String phoneEnd="5714";
            SyncUserInfoModel model=new SyncUserInfoModel();
            model.setThirdId(SnowflakeUtils.getNextIdStr());
            model.setAreaCode("86");
            model.setTelephone("1584292"+phoneEnd);
            model.setAccount("66668888");
            model.setPassword(Md5Util.md5Hex("111111"));
            model.setNickname("api测试"+phoneEnd);
            model.setDescription("欢迎加好友");
            model.setSex(1);
            model.setBirthday(DateUtil.parseDate("1995-05-27").getTime()/1000);
            model.setPayPassWord(Md5Util.md5Hex("111222"));

            Map<String,String> extension=new HashMap<>();
            extension.put("thirdId",model.getThirdId());
            extension.put("account",model.getAccount());
            extension.put("nickname",model.getNickname());
            model.setExtension(JSON.toJSONString(extension));

            requestClient.syncRegisterUser(model);




        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void requestUserInfoTest() {
        try {
            SystemRequestClient requestClient=new SystemRequestClient(apiUrl,publicKey,apiSecret);
            requestClient.setAccess_token(token);
            //requestClient.requestAccess_token();
            System.out.println("token  "+requestClient.getAccess_token());


            String phoneEnd="5714";


             String thirdId="282533405104619520";

            JSONObject resultJson =requestClient.queryUserInfo(thirdId);

            SyncUserInfoModel model=new SyncUserInfoModel();
            model.setThirdId(thirdId);
            model.setPassword(Md5Util.md5Hex("123456"));
            model.setNickname("api测试-"+phoneEnd);
            model.setDescription("欢迎加好友,我是"+phoneEnd);
            Map<String,String> extension=new HashMap<>();
            extension.put("thirdId",model.getThirdId());
            extension.put("account",model.getAccount());
            extension.put("nickname",model.getNickname());
            model.setExtension(JSON.toJSONString(extension));
            requestClient.syncUserInfo(model);

            Integer userId=100055;
            //requestClient.syncLoginSession(userId,"android",thirdId,"zh_cn",65535);

            //requestClient.clearLoginSession(userId,"android");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void generateKeyPair() {
        try {
            Map<String, String> keyPair = RSA.generateKeyPair(1024);
            System.out.println("privateKey    " + keyPair.get("privateKey"));

            System.out.println("publicKey    " + keyPair.get("publicKey"));


            System.out.println("modulus    " + keyPair.get("modulus"));

            System.out.println("apiSecret "+ StringUtil.randomUUID());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void jwtTest() {
        try {

            DateTime now = DateTime.now();
            DateTime newTime = now.offsetNew(DateField.HOUR, 12);

            Map<String,Object> payload = new HashMap<String,Object>();
            //签发时间
            payload.put(JWTPayload.ISSUED_AT, now);
            //过期时间
            payload.put(JWTPayload.EXPIRES_AT, newTime);
            //生效时间
            payload.put(JWTPayload.NOT_BEFORE, now);
            //载荷
            payload.put("userId", "1000020");
            payload.put("name", "lidaye");

            byte[] keys = Base64.decode(privateKey);

            String oldtoken = JWTUtil.createToken(payload, keys);

            System.out.println(" oldtoken "+oldtoken);



            String token=AES.encryptBase64(oldtoken.getBytes(StandardCharsets.UTF_8),keys);

            System.out.println(" sign token "+token+"  "+token.equals(oldtoken));
            token=AES.decryptStringFromBase64(token,keys);
            System.out.println(" decrypt token "+token+"  "+token.equals(oldtoken));
            JWT jwt = JWTUtil.parseToken(token);

            System.out.println(" verify "+JWTUtil.verify(token, keys));


            System.out.println(" parseToken "+jwt.getPayloads().toString());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void demoTest() {
        try {

            String nextIdStr = SnowflakeUtils.getNextIdStr();
            System.out.println(Hex.encodeHexString(nextIdStr.getBytes(StandardCharsets.UTF_8)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}