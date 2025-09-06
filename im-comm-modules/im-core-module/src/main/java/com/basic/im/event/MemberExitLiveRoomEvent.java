package com.basic.im.event;

/**
 * 用户退出直播间时间，包含群踢出用户，群解散
 */
public class MemberExitLiveRoomEvent {

    private Integer userId; // 用户 ID
    private String roomId; // 群组 ID


    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getRoomId() {
        return roomId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }
}
