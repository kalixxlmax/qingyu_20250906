package com.basic.im.mpserver.utils;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

/**
 * @author xie yuan yang
 * @Date Created in 2019/9/17 10:43
 * @description
 * @modified By:  验证手机号码
 */
public class MobileValidateUtils {

    private static PhoneNumberUtil phoneNumberUtil = PhoneNumberUtil.getInstance();

    /**
     * @Description 国内手机号校验
     * @Date 12:11 2020/4/26
     **/
    public static boolean checkMobileNumber(String mobileNumber) {

        if (mobileNumber.length() != 11){
            return false;
        }

        boolean flag = false;
        try {
            Pattern regex = compile("^1[345789]\\d{9}$");
            Matcher matcher = regex.matcher(mobileNumber);
            flag = matcher.matches();
        } catch (Exception e) {
            e.printStackTrace();
            flag = false;

        }
        return flag;
    }

    /**
     * @Description 根据区号判断是否是正确的电话号码
     * @param countryCode :默认国家码
     *           return ：true 合法  false：不合法
     * @paramphoneNumber :带国家码的电话号码
     * @Date 12:11 2020/4/26
     **/
    public static boolean isPhoneNumberValid(String phoneNumber, String countryCode) {
        PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
        try {
            Phonenumber.PhoneNumber numberProto = phoneUtil.parse(phoneNumber, countryCode);
            return phoneUtil.isValidNumber(numberProto);
        } catch (NumberParseException e) {
            System.err.println("isPhoneNumberValid NumberParseException was thrown: " + e.toString());
        }
        return false;
    }

    public static boolean checkPhoneNumber(String phoneNumber, Integer countryCode) {
        long phone = Long.parseLong(phoneNumber);
        Phonenumber.PhoneNumber pn = new Phonenumber.PhoneNumber();
        pn.setCountryCode(countryCode);
        pn.setNationalNumber(phone);
        return phoneNumberUtil.isValidNumber(pn);
    }

}
