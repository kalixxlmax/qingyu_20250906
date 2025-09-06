package com.basic.im.security.handle;

import com.basic.im.vo.JSONMessage;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Component;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @Description: TODO （权限异常处理器）
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Component
public class CustomAccessDeniedHandler implements AccessDeniedHandler {
    @Override
    public void handle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, AccessDeniedException e) throws IOException, ServletException {
        System.out.println("有错误！！！" + e.getMessage());
        //判断是不是异步请求
        String type = httpServletRequest.getHeader("X-Requested-With");
        if ("XMLHttpRequest".equals(type)){
            httpServletResponse.setContentType("application/json;charset=UTF-8");
            if ("Access is denied".equals(e.getMessage())){
                httpServletResponse.getWriter().print(JSONMessage.failure("权限不足"));
            }else{
                httpServletResponse.getWriter().print(JSONMessage.failure(e.getMessage()));
            }
        }else{
            httpServletRequest.getRequestDispatcher("/pages/console/authorizationFailed.html").forward(httpServletRequest,httpServletResponse);
        }
    }
}
