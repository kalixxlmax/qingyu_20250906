package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.Emoji;
import org.bson.types.ObjectId;

import java.util.List;

public interface CollectionDao extends IMongoDAO<Emoji,Integer> {

    void addEmoji(Emoji emoji);

    void deleteEmoji(ObjectId emojiId,Integer userId);

    void deleteEmoji(String collectMsgId,int userId);

    Emoji getEmoji(ObjectId emojiId,Integer userId);

    Emoji getEmoji(String msgId,Integer userId);

    Emoji getEmoji(String collectMsgId,int userId);

    Emoji getEmoji(Integer userId,String url);

    Emoji getEmoji(String msg, int type, Integer userId, String msgId);

    List<Emoji> queryEmojiList(Integer userId,int type,Integer pageIndex,Integer pageSize);
    List<Emoji> queryEmojiList(Integer userId,int type);


    List<Emoji> queryEmojiListOrType(Integer userId,Integer pageIndex,Integer pageSize);

    Emoji queryEmojiByUrlAndType(String url,int type,Integer userId);

    List<Emoji> queryListByKey(Integer userId, String keyword, int pageSize, int pageIndex);

}
