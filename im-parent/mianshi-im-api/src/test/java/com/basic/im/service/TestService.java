package com.basic.im.service;


import com.alipay.api.DefaultAlipayClient;
import com.alipay.api.domain.AlipayTradeAppPayModel;
import com.alipay.api.request.AlipayTradeAppPayRequest;
import com.alipay.api.response.AlipayTradeAppPayResponse;
import com.alipay.util.AliPayUtil;
import com.basic.im.admin.service.impl.AdminManagerImpl;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.room.dao.RoomMemberDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.mianshi.Application;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest(classes= Application.class)
public class TestService implements ApplicationContextAware {

    private ApplicationContext context = null;


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }



    @Autowired
    private FriendsManagerImpl friendsManager;


    @Autowired
    private RoomManagerImplForIM roomManagerImplForIM;
    @Autowired
    private RoomMemberDao roomMemberDao;

    @Test
    public void TestUpdateDeadLine() {

        roomMemberDao.updateRoomMemberDeadLine(new ObjectId("628ed4fe6cf74a73df419c1b"),
                100001,123);

    }

    @Test
    public void TestAlipay() {
        String appid = "2021002183687457";
        String callBackUrl = "2021002183687457";
        String pid = "2088241562937641";
        String app_private_key = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOTiXDQ94A/jeVwMtc6a6SGE8vrNv2Q4HT6XF1ColYUz3wIU6pm9uqILbTjoAAZ8Ra2mwnyXks6NOfXdJvsvUiqAmezCUGeClcNHcA4s1BJv5YOEwlZzA3aOABPHJkd07vB73zeChwKxeAmUcWlQgHLHdN/O8wA0FlWdA6TPOIZmVpeNHuJpIrPDNameH076P3GKCudRAqme3bfuM7Nmk5a/inXuVlMfi/jEPJuDPCocUPK00mIfQzbagssLjY6AQfpwbbzpZ3UnbQEHhzdPJ34xK1YcpiocdphLZ946kG7U2SOtwmNgsUzDON6jPkpc7Mk/39plQnfTQxsjrP54yvAgMBAAECggEAboBfK7LLU57W6QhmJPMpxSjuKSweeU55WKc+VSF6pWuYzmkYuDI6O2rgYnp+RzlNbBh4O+tnU022JDNdxuSE9rYoyPyxjECKbs/POhNBO3eGXnxxFMurgNJUwXS+FN/u0ItGDAW9Ky95nDD9abQTo7wGfyhOeVoVChNMPB4Bh3nS9lToyRb4r+JaczZSshkLaoS95dh/tjvY/Tt1uOxiWK88IogjHV/uD+JK9EFuAN7xYdrvnbtDB5xAuKYQzLjafEBlUOOg1cjv5p5IWaQrSKusXpBcJktuNl7m7qMixyOUwt5Px8nsHATjYVrSEru2qb0KAot4vRU59KKVrof2UQKBgQDqKT8Llc6SpoQ5lekRFQ9UqkJiBZHpmikPcYg4QYo3SApuH5bhPhZYKfpU8iBYFqXoTcegjck+LgybTp7ea1tNKaAecJmU9nez1GxP7Ac1WA6HId6E+6n8hZwbZXhiIe1LgbfKm+oit+54nqlGGCZOw+1aX5C6ixhLxSKJGU7g9wKBgQDhi9Lz0NwDssIJjZwRiXNqt5Dz3hXnldkcXaohSLJoF3Pei6FDxYlyXRs+Upk0k5Fe49pYzZAq2l0s5BZ+2KAcsfbDxS+y3n0fw/lJpXMOR7Fxzxb/I2zEB3e0E6COJpTvgECJ5JkyoEJWz0QN7U1/ZWGhBFr9O/CFfZnHIaZ8CQKBgQC5EZJfgK48n9j/fmkvdBqiytIekNJq88miCBQmij5IROrhcJwaN60HT14nxxojJPtJaDfvwEAeodya3v+XQPhvOwfvJwVrg4TGH4zfeeeUlJ0e1nj9V75Y8+yWhpMn1DKtK7/sJWBewkp7EMMuwCV+uUGd3MwRc5/nOs551wPNrwKBgGq5ghmS8/jgBkcE9huqQa32rKVoqk7KLlDo1U8omhr9NfvRcH6kI1S1T+XeqdYiGcCf6rroklZku4A5cZeWI7VQVKbwUJDADM8SKYy6uEXCkOl8h7I07guYSQEx5GA/S2xE8MUPIZq+EIYJT3eTmX811sudRvza9wYDl9eNSb0ZAoGBAMn/mFy163Hgx+5zltWv5hEwfe+cqFsfUCSXLdDigqfKjlxbqTP3RFwrUykfm7fg9GZ7NaVah48uBi0CFEq6qAeKKMm0fCpbERzMqtE95cIULTm67le8h3Bm8wkWJU500sb6VXFRqF/VPpfrw0ZCJrhjNZHCsjkAa/M2MIPqGDEE";
        String charset = "utf-8";
        String alipay_public_key = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1u3eKlPycKC9h0ixiV9nVGeeNgJKOEjYvVEbI1afgfqQ/YOweu3TiEaR84X0ejUDpYputwwNw/G9O+64n3SsSBrtSdnkaONhaS7EiqlDK5FMnk1ukP1eXEP0+YV/mkAubxLwCPrmJkDhL9RbE/5RfRzdIPaQBtp/unobs9vQTki9e7mo92YuTdGWERS/em5qsWbK5TBpH8QR1I8o9JzuSOGG8mMYXx5ZzQkk+zmdlsV4wb0sb8AeSiT59VDMNT4NLON/zgvE2iUpGZLHBOtp9wCqcwBIsh1of2Y9X9XomOhptiKBGmkM7UYCNaI3/NCdtGe3DpEq4UhR7b23TGuFowIDAQAB";
        DefaultAlipayClient alipayClient = new DefaultAlipayClient("https://openapi.alipay.com/gateway.do",
                appid, app_private_key, "json", charset, alipay_public_key, "RSA2");

        AlipayTradeAppPayRequest request = new AlipayTradeAppPayRequest();
        //SDK已经封装掉了公共参数，这里只需要传入业务参数。以下方法为sdk的model入参方式(model和biz_content同时存在的情况下取biz_content)。
        AlipayTradeAppPayModel model = new AlipayTradeAppPayModel();

        model.setBody("付费加入群聊");
        model.setSubject("付费加入群聊");
        model.setOutTradeNo(AliPayUtil.getOutTradeNo());
        model.setTimeoutExpress("30m");
        model.setTotalAmount("1");
        model.setProductCode("QUICK_MSECURITY_PAY");
//			model.setGoodsType("0");
        request.setBizModel(model);
        request.setNotifyUrl(callBackUrl);
        try {
            //这里和普通的接口调用不同，使用的是sdkExecute
            AlipayTradeAppPayResponse response = alipayClient.sdkExecute(request);
            System.out.println("返回order  "+response.getBody());//就是orderString 可以直接给客户端请求，无需再做处理。
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void TestFriendsServices() {

        friendsManager.ModifyFriendsChatSercetData();

    }


    @Test
    public void  destoryRooms(){

        //8618738880745  的userId 为 10002092
        int targetUserId = 10002092;
        //查找用户创建的群组以及管理的群组
        List<Integer> allMemberIds = roomManagerImplForIM.getUsersCreateRoomAndManagerRoom(targetUserId);

    }

    @Autowired
    private AdminManagerImpl adminManager;

   /* @Test
    public  void  updateNotDeleteUserData(){
        adminManager.destoryAppointUser(10002092);

    }*/


    /**
     * 更新好友的消息保存时长
     */
    @Test
    public void updateFriendMsgSaveTime(){
        String roomJid = "4cf686f645d148aa97c8426824325ee9";
        int userId = 10000;
        List<Room.Member> roomMembers = roomManagerImplForIM.findMemberByRoomJid(userId, roomJid,0,100);

        for (int i=0;i<roomMembers.size();i++){
          for (int j=0;j < roomMembers.size(); j++ ){
              //排除当前用户自己
              if(roomMembers.get(i).getUserId() == roomMembers.get(j).getUserId() ){
                  continue;
              }
              System.out.println("=#### ==============>>> "+roomMembers.get(i).getNickname() +"==== > "+roomMembers.get(j).getNickname());
              Friends friends = friendsManager.getFriends(roomMembers.get(i).getUserId(), roomMembers.get(j).getUserId());
             if(null==friends){
                 continue;
             }
              friends.setChatRecordTimeOut(-1);
              friendsManager.updateFriends(friends);
          }

        }



    }




}
