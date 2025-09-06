package com.basic.translate.controller;

import com.basic.common.response.JSONBaseMessage;
import com.basic.commons.constants.CommConstants;
import com.basic.translate.factory.TranslateServiceFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class TranslateController {

    @RequestMapping(value = "/translate")
    @ResponseBody
    public JSONBaseMessage translate(@RequestParam String content,
                                     @RequestParam String from,
                                     @RequestParam String to,
                                     @RequestParam(defaultValue = "") String messageId) {
        return TranslateServiceFactory.getTranslateService().getTranslatedContent(content, from, to,messageId);
    }
}
