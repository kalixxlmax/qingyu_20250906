package com.basic.im.room.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class MemberNameDTO {
    private String nickname;
    private String remarkName;
}
