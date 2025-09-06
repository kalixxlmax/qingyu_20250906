/*
 * Copyright 2020. Huawei Technologies Co., Ltd. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.basic.im.push.service.hwpush.examples;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.push.service.hwpush.android.AndroidNotification;
import com.basic.im.push.service.hwpush.android.BadgeNotification;
import com.basic.im.push.service.hwpush.android.ClickAction;
import com.basic.im.push.service.hwpush.exception.HuaweiMesssagingException;
import com.basic.im.push.service.hwpush.message.AndroidConfig;
import com.basic.im.push.service.hwpush.message.Message;
import com.basic.im.push.service.hwpush.message.Notification;
import com.basic.im.push.service.hwpush.messaging.HuaweiMessaging;
import com.basic.im.push.service.hwpush.model.Importance;
import com.basic.im.push.service.hwpush.model.Urgency;
import com.basic.im.push.service.hwpush.model.Visibility;
import com.basic.im.push.service.hwpush.reponse.SendResponse;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendNotifyMessage extends SendHWMessage {

    /**
     * 华为普通推送
     * send notification message
     *
     * @throws HuaweiMesssagingException
     */
    public static void sendNotification(MsgNotice notice, User.DeviceInfo deviceInfo, boolean flag, boolean initFlag) throws HuaweiMesssagingException {
        HuaweiMessaging huaweiMessaging = HuaweiMessaging.getInstance(getAPPInfo(deviceInfo.getPackName(), initFlag));
        //log.info("appId:{} ,appSecret :{} ,notice :{},token :{}",getPushConfig(deviceInfo.getPackName()),getPushConfig(deviceInfo.getPackName()).getXmAppSecret(),notice.toString(),deviceInfo.getPushToken());
        Notification notification = Notification.builder().setTitle(notice.getTitle())
                .setBody(notice.getText())
                .build();

        // 打开自定义app页面
        String url= null;
        if(notice.getIsGroup()){
            url="intent://"+deviceInfo.getPackName()+"/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId="+notice.getFrom()+";S.roomJid="+notice.getObjectId()+";end";
        }else{
            url="intent://"+deviceInfo.getPackName()+"/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId="+notice.getFrom()+";end";
        }
        AndroidNotification androidNotification = AndroidNotification.builder().setIcon("/raw/ic_launcher2")
                .setColor("#AACCDD")
                .setSound("/raw/shake")
                .setDefaultSound(true)
                .setTag("tagBoom")
                .setClickAction(ClickAction.builder().setType(1).setIntent(url).build())
                .setBodyLocKey("M.String.body")
                .addBodyLocArgs("boy").addBodyLocArgs("dog")
                .setTitleLocKey("M.String.title")
                .addTitleLocArgs("Girl").addTitleLocArgs("Cat")
                .setChannelId(flag ? "20200408" :"20200302")
//                .setNotifySummary("some summary")// 安卓通知栏消息简要描述
//                .setMultiLangkey(multiLangKey)
                .setStyle(1)
                .setBigTitle(notice.getTitle())
                .setBigBody(notice.getText())
                .setAutoClear(86400000)
                .setNotifyId(486)
                .setGroup("Group1")
                .setImportance(Importance.HIGH.getValue())
//                .setLightSettings(lightSettings)
                .setBadge(BadgeNotification.builder().setAddNum(1).setBadgeClass("Classic").build())
                .setVisibility(Visibility.PUBLIC.getValue())
                .setForegroundShow(true)
                .build();

        AndroidConfig androidConfig = AndroidConfig.builder().setCollapseKey(-1)
                .setUrgency(Urgency.HIGH.getValue())
                .setTtl("10000s")
                .setBiTag("the_sample_bi_tag_for_receipt_service")
                .setNotification(androidNotification)
                .build();

        Message message = Message.builder().setNotification(notification)
                .setAndroidConfig(androidConfig)
                .addToken(deviceInfo.getPushToken())
                .build();
        SendResponse response = huaweiMessaging.sendMessage(message);
        log.info("response :{} ", JSONObject.toJSONString(response));
    }
}
