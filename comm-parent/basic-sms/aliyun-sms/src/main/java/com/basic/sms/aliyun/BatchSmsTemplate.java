package com.basic.sms.aliyun;

import lombok.*;

import java.util.List;
import java.util.Map;

/**
 * 阿里云 SMS 短信模板.
 *
 * @author cn-src
 */
@Builder(builderClassName = "Builder", toBuilder = true)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class BatchSmsTemplate {

    private List<String> signNames;
    private String templateCode;
    private List<Map<String, String>> templateParams;
    private List<String> phoneNumbers;

}