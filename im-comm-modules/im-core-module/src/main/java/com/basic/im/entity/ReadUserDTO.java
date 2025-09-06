package com.basic.im.entity;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ReadUserDTO {
    private Integer userId;
    private String nickname;
    private Integer hiding;
}
