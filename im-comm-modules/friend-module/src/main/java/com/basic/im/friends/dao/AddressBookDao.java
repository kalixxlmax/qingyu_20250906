package com.basic.im.friends.dao;

import com.basic.im.friends.entity.AddressBook;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

public interface AddressBookDao extends IMongoDAO<AddressBook, ObjectId> {

    void addAddressBookList(List<AddressBook> bookList);

    void addAddressBook(AddressBook addressBook);

    void deleteAddressBook(int userId, String[] deleteArray);

    List<AddressBook> queryListByUserId(int userId, int pageIndex, int pageSize);

    List<AddressBook> queryListByUserId(int userId);

    List<AddressBook> queryListByToTelephone(String toTelephone);

    List<AddressBook> queryRegisterEdList(int userId, int registerEd, int pageIndex, int pageSize);

    List<String> queryTelephoneListToTelephone(String toTelephone);

    long getAddressBookAllCount(int userId, String toTelephone);

    AddressBook getAddressBook(String telephone, int toUserId);

    void deleteAddressBook(String telephone, String toTelephone, Integer userId);

    void deleteAddressBook(int userId, int toUserId);

    List<AddressBook> getAddressBookAll(String telephone);

    void updateAddressBook(String telephone);

    void updateAddressBook(String telephone, int userId, String nickName, long registerTime, int status);
}
