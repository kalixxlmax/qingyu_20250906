package com.basic.im.room.vo;

import com.basic.im.model.BasePoi;
import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @author wcl
 * @version V1.0
 * @Description: TODO(群组搜索)
 *
 * @date 2020/6/18 12:03
 */
@Data
@Accessors(chain = true)
public class NearByRoom extends BasePoi {

    private List<ObjectId> roomIds;// roomId 列表

    private int s;// 状态  1:正常, -1:被禁用

    private String roomName;// 房间昵称

}
