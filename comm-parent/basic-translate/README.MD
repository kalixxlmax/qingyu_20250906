## 视酷通用翻译模块
> 目前支持 百度、有道、搜狗翻译，通过 translator.type 进行修改
```properties
# 例如配置为百度,其他为 sougou、youdao
translator.type=baidu
```

## 如何使用
> 引入依赖
```xml
 <dependency>
    <groupId>com.basic.common</groupId>
    <artifactId>basic-translate</artifactId>
    <version>1.0</version>
</dependency>
```

## 进行调用
> 直接引入即可，已默认定义了 Controller 提供给前端访问
> http://192.168.0.185:8092/translate?content=%E5%8E%9F%E6%96%87&from=auto&to=zh

### 参数说明
|  参数名称   | 参数类型  | 参数作用  |
|  ----  | ----  | ----  |
| content  | String | 要翻译的文本 |
| from  | String | 要翻译文本的原有语言，可以通过 auto 指定为自动识别 |
| to  | String | 目标语言 |
| messageId  | String | 消息id(非必传) |


### 返回内容
|  参数名称   | 参数类型  | 参数作用  |
|  ----  | ----  | ----  |
| resultCode  | int | 执行结果编码 |
| resultMsg  | String | 提示信息 |
| currentTime  | String | 完成时间 |
| data  | String | 结果数据 |


### data 参数说明
|  参数名称   | 参数类型  | 参数作用  |
|  ----  | ----  | ----  |
| messageId  | String | 消息Id |
| translation  | String | 翻译后的文本 |

### 百度翻译语言列表

|  语言简写   | 语言名称  |
|  ----  | ----  |
| zh  | 中文 |
| en  | 英文 |
| yue  | 粤语 |
| wyw  | 文言文 |
| jp  | 日语 |
| kor  | 韩语 |
| fra  | 法语 |
| spa  | 西班牙语 |
| ara  | 阿拉伯语 |
| ru  | 俄语 |
| pt  | 葡萄牙语 |
| de  | 德语 |
| it  | 意大利语 |
| el  | 希腊语 |
| nl  | 荷兰语 |
| pl  | 波兰语 |
| bul  | 保加利亚语 |
| est  | 爱沙尼亚语 |
| dan  | 丹麦语 |
| fin  | 芬兰语 |
| cs  | 捷克语 |
| rom  | 罗马尼亚语 |
| slo  | 斯洛文尼亚语 |
| swe  | 瑞典语 |
| hu  | 匈牙利语 |
| cht  | 繁体中文 |
| vie  | 越南语 |


### 配置项
```properties
# API文档 https://fanyi-api.baidu.com/api/trans/product/apidoc
translator.baidu.enabled=true
translator.baidu.appid=your appid
translator.baidu.secretKey= your secretKey
translator.baidu.zh=zh
translator.baidu.en=en
translator.baidu.url=http://api.fanyi.baidu.com/api/trans/vip/translate?appid=%s&q=%s&from=%s&to=%s&salt=%s&sign=%s
translator.baidu.salt=32768

# API文档 https://deepi.sogou.com/doccenter/texttranslatedoc
translator.sogou.enabled=true
translator.sogou.appid=your appid
translator.sogou.salt=1508404016012
translator.sogou.secretKey=your secretKey
translator.sogou.url=http://fanyi.sogou.com:80/reventondc/api/sogouTranslate
translator.sogou.zh=zh-CHS
translator.sogou.en=en


# API文档 https://ai.youdao.com/DOCSIRMA/html/%E8%87%AA%E7%84%B6%E8%AF%AD%E8%A8%80%E7%BF%BB%E8%AF%91/API%E6%96%87%E6%A1%A3/%E6%96%87%E6%9C%AC%E7%BF%BB%E8%AF%91%E6%9C%8D%E5%8A%A1/%E6%96%87%E6%9C%AC%E7%BF%BB%E8%AF%91%E6%9C%8D%E5%8A%A1-API%E6%96%87%E6%A1%A3.html
translator.youdao.enabled=true
translator.youdao.appid=your appid
translator.youdao.secretKey=your secretKey
translator.youdao.zh=zh-chs
translator.youdao.en=en
translator.youdao.url=http://openapi.youdao.com/api
```


### 直接使用

```java
TranslateServiceFactory.getTranslateService().getTranslatedContent("translate", "auto", "zh");
```
> 其他，待补充