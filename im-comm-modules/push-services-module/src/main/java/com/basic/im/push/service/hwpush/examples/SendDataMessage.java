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
import com.basic.im.push.service.hwpush.exception.HuaweiMesssagingException;
import com.basic.im.push.service.hwpush.message.AndroidConfig;
import com.basic.im.push.service.hwpush.message.Message;
import com.basic.im.push.service.hwpush.messaging.HuaweiMessaging;
import com.basic.im.push.service.hwpush.model.Urgency;
import com.basic.im.push.service.hwpush.reponse.SendResponse;
import com.basic.im.user.entity.User;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendDataMessage extends SendHWMessage {
   /**
    * @throws HuaweiMesssagingException
    * send data message
    */
    public static void sendTransparent(User.DeviceInfo deviceInfo,boolean initFlag) throws HuaweiMesssagingException {
        HuaweiMessaging huaweiMessaging = HuaweiMessaging.getInstance(getAPPInfo(deviceInfo.getPackName(), initFlag));

        AndroidConfig androidConfig = AndroidConfig.builder().setCollapseKey(-1)
                .setUrgency(Urgency.HIGH.getValue())
                .setTtl("10000s")
                .setBiTag("the_sample_bi_tag_for_receipt_service")
                .build();

        Message message = Message.builder()
                .setData("{'k1':'v1', 'k2':'v2'}")
                .setAndroidConfig(androidConfig)
                .addToken(deviceInfo.getPushToken())
                .build();

        SendResponse response = huaweiMessaging.sendMessage(message);
        log.info("透传 response :{} ", JSONObject.toJSONString(response));
    }
}
