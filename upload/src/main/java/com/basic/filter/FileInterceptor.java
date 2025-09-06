package com.basic.filter;

import com.basic.commons.vo.JMessage;
import com.basic.commons.utils.FileValidator;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@Configuration
public class FileInterceptor extends HandlerInterceptorAdapter {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (!FileValidator.checkFileSuffix(request)){
            response.getWriter().println(JMessage.errorOf(null).setResultMsg(FileValidator.FILE_NOT_SUPPORT));
            return false;
        }else{
            return true;
        }
    }
}