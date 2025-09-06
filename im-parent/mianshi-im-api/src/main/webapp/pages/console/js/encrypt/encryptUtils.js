var EncryptUtils={
    AES:{
        aesIvKey:null,
        getAesIvKey:function(){
            if(!this.aesIvKey){
                let ivArr=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16];
                let iv=EncryptUtils.getStrFromBytes(ivArr);
                this.aesIvKey=CryptoJS.enc.Utf8.parse(iv);
            }
            return this.aesIvKey;
        },
        encrypt:function (value,key) {
            return CryptoJS.AES.encrypt(value, key, this.getOption(this.getAesIvKey()));
        },
        decrypt:function (value,key) {
            return CryptoJS.AES.decrypt(value, key,this.getOption(this.getAesIvKey()));
        },
        option:null,
        getOption:function(ivKey){
            if(!this.option){
                this.option={
                    iv: ivKey,
                    mode:CryptoJS.mode.CBC,
                    padding:CryptoJS.pad.Pkcs7,
                }
            }

            return this.option;
        },
    },



    encryptAES:function(value,key){
        return this.AES.encrypt(value,key);
    },

    decryptAES:function(value,key){
        return this.AES.decrypt(value,key);
    },
    encryptMac:function(value,key){
        return CryptoJS.HmacMD5(value, key);
    },
    encryptMacToBase64:function(value,key){
        let mac= this.encryptMac(value, key);
        return CryptoJS.enc.Base64.stringify(mac);
    },
    /*
     字节数组转换为字符串
     */
    getStrFromBytes:function (arr) {
        let r = "";
        /* for (let i in arr) {
         r += String.fromCharCode(i);
         }*/
        for(let i=0;i<arr.length;i++){
            r += String.fromCharCode(arr[i]);
        }
        //console.log(r);
        return r;
    },

    /*
     DH 算法
     */
    DH:{
        /*
         公钥转换截取 字符串
         */
        beginPubKey:"3056301006072a8648ce3d020106052b8104000a034200",

        /*
         私钥转换截取 字符串 前缀
         */
        beginPriKey:"30740201010420",
        /*
         私钥转换截取 字符串 后缀
         */
        endPriKey:"A00706052B8104000AA144034200",
        ecdh:null,/*DH 对象*/
        getECDH:function(){
            if(!this.ecdh)
                this.ecdh = new elliptic.ec('secp256k1');
            return this.ecdh;
        },
        /*
         创建DH 密钥对
         */
        genKeyPair:function(){
            return this.getECDH().genKeyPair();
        },
        /*
         js DH公钥对象转换为 服务器 base64 公钥字符串
         pub  js DH公钥对象
         */
        encryptPublicKey:function(pub){
            let pubHexStr= this.beginPubKey+pub.encode('hex');
            let pubByte=CryptoJS.enc.Hex.parse(pubHexStr);
            return CryptoJS.enc.Base64.stringify(pubByte);
        },
        /*
         服务器 公钥字符串 转换为 js 公钥
         pubBase64Str base64 的公钥字符串
         */
        decryptPublicKey:function(pubBase64Str){
            let byteBase64= CryptoJS.enc.Base64.parse(pubBase64Str);
            let hexString = CryptoJS.enc.Hex.stringify(byteBase64);
            //console.log("hexString ",hexString);
            hexString=hexString.substring(46,hexString.length);
            return this.getECDH().keyFromPublic(hexString,"hex");
        },
        /*
         js DH私钥对象转换为 服务器 base64 私钥字符串
         keyPair  js 密钥对象
         */
        encryptPrivateKey:function(keyPair){
            let pri=keyPair.getPrivate('hex');
            let pub=keyPair.getPublic('hex');
            let priHexStr= this.beginPriKey+pri+this.endPriKey+pub;
            let priByte=CryptoJS.enc.Hex.parse(priHexStr);
            return CryptoJS.enc.Base64.stringify(priByte);
        },
        /*
         服务器 私钥字符串 转换为 js 密钥
         priBase64Str base64 的私钥字符串
         */
        decryptPrivateKey:function(priBase64Str){
            let byteBase64= CryptoJS.enc.Base64.parse(priBase64Str);
            let hexString = CryptoJS.enc.Hex.stringify(byteBase64);
            //console.log("hexString ",hexString);
            hexString=hexString.substring(14,hexString.length);
            hexString=hexString.substring(0,64);
            return this.getECDH().keyFromPrivate(hexString,"hex");
        },
    },

    /*
     构建登陆密码
     pwd 密文密码
     */
    buildLoginPassword:function(pwd){
        let md5pwd=CryptoJS.MD5(pwd);
        let md5Str=CryptoJS.enc.Base64.stringify(md5pwd);
        let encryptAES=EncryptUtils.encryptAES(CryptoJS.enc.Base64.parse(md5Str),md5pwd);
        let md5Aes=CryptoJS.MD5(encryptAES.ciphertext);
        let encode =CryptoJS.enc.Hex.stringify(md5Aes);
        return encode;
    },

    encryptAES_StrToStr_test:function(value,key){
        let result= this.AES.encrypt(value,key);
        return CryptoJS.enc.Base64.stringify(result.ciphertext);
    },

    /*
     key 公钥 or 私钥
     key 不传值 则创建一对新的密钥对
     */
    generatedRSAKey:function(key){
        /*创建 RSA 密钥对*/
        let rsaKeyPair = new JSEncrypt({default_key_size:this.rsaKeySize});
        if(!key){
            rsaKeyPair.getKey();
        }else {
            rsaKeyPair.setKey(key);
        }
        return rsaKeyPair;
    },
    /*
     创建DH 密钥对
     */
    genDHKeyPair:function(){
        return this.DH.genKeyPair();
    },
    /*
     js DH私钥对象转换为 服务器 base64 私钥字符串
     keyPair  js 密钥对象
     */
    encryptDHPrivateKey:function(keyPair){
        return this.DH.encryptPrivateKey(keyPair);
    },
    /*
     js DH公钥对象转换为 服务器 base64 公钥字符串
     pub  js DH公钥对象
     */
    encryptDHPublicKey:function(pub){
        return this.DH.encryptPublicKey(pub);
    },



    /**
     * 创建RSA和DH的公私钥方法
     * @param obj.repassword 明文密码
     */
    createRSAandDHSecretKey : function(obj){
        console.log("createRSAandDHSecretKey :"+JSON.stringify(obj));
        // rsa 秘钥对
        let keyPrair = this.generatedRSAKey();
        let rsaPrivateKey = keyPrair.getPrivateKeyB64();
        let rsaPublicKey = keyPrair.getPublicKeyB64();
        console.log("rsa 私钥 ："+rsaPrivateKey);
        rsaPrivateKey = this.encryptAES_StrToStr_test(CryptoJS.enc.Base64.parse(rsaPrivateKey), CryptoJS.MD5(obj.repassword));
        obj.rsaPublicKey = rsaPublicKey;
        obj.rsaPrivateKey = rsaPrivateKey;
        // dh秘钥对
        let ecdh = this.genDHKeyPair();
        let dhPrivateKey=this.encryptDHPrivateKey(ecdh);
        let dhPublicKey=this.encryptDHPublicKey(ecdh.getPublic());
        console.log("regedit : "+dhPrivateKey);
        console.log("webpub : "+dhPublicKey);
        dhPrivateKey = this.encryptAES_StrToStr_test(CryptoJS.enc.Base64.parse(dhPrivateKey), CryptoJS.MD5(obj.repassword));
        console.log("webpri :"+dhPrivateKey);
        obj.dhPublicKey = dhPublicKey;
        obj.dhPrivateKey = dhPrivateKey;
        delete obj.repassword;// 明文密码
        return obj;
    },

    /**
     * 重置密码 mac验参
     * @param obj
     */
    resetPasswordV1Param : function(obj){
        console.log("resetPasswordV1Param obj : "+JSON.stringify(obj));
        let newPassword = this.buildLoginPassword(obj.repassword);// 新密码
        let code = CryptoJS.enc.Base64.parse(newPassword).toString().substring(0,32);// 24 toString = 16进制48*2；16
        let ward = CryptoJS.enc.Hex.parse(code);
        console.log("ward : "+code);
        console.log("apiKey : "+localStorage.getItem("apiKey"));
        let byteAesPwdTest = this.encryptAES_StrToStr_test(CryptoJS.enc.Utf8.parse(localStorage.getItem("apiKey")),ward);
        console.log("key :"+byteAesPwdTest);
        obj.mac = this.encryptMacToBase64(CryptoJS.enc.Base64.parse(byteAesPwdTest),CryptoJS.enc.Utf8.parse(obj.telephone));
        console.log("mac :"+obj.mac);
        obj.newPassword = newPassword;
        obj = this.createRSAandDHSecretKey(obj);
        return obj;
    },

    /**
     *   用于重置密码相关操作
     *   登录加固后校验调用忘记密码的接口版本
     */
    isSupportSecureChat:function (obj,callback) {
        Common.invoke({
            type:"POST",
            url : request('/console/authkeys/isSupportSecureChat'),
            data : obj,
            success : function(result) {
                if (1 == result.resultCode) {
                    if(callback)
                        callback(result.data);
                }
            },
            error : function(result) {
            }
        });
    },

    /**
     *  登录加固后忘记密码
     */
    resetPasswordV1:function (obj,callback) {
        obj = this.resetPasswordV1Param(obj);
        Common.invoke({
            url : request('/console/password/reset/v1'),
            data : obj,
            success : function(result) {
                if (1 == result.resultCode) {
                    layui.layer.msg("重置密码成功!",{"icon":1});
                    if(callback)
                        callback(result.data);
                }
            },
            error : function(result) {
            }
        });
    },

    /**
     *  重置密码
     */
    resetPassword:function (obj,callback) {
        Common.invoke({
            url : request('/user/password/reset'),
            data : obj,
            success : function(result) {
                if (1 == result.resultCode) {
                    if(callback)
                        callback(result.data);
                }
            },
            error : function(result) {
            }
        });
    },



}




