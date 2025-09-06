package com.basic.im.manual.entity;

import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Document(value = "collection_account")
public class CollectionAccount {
    @Id
    private ObjectId id;

    private String accountName;

    private String account;

}
