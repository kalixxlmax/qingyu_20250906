package com.basic.im.security.handle;

import com.basic.im.comm.ex.ServiceException;
import com.basic.im.vo.JSONMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Slf4j
@RestControllerAdvice
public class RestExceptionHandler {
    
    /**
     * @Description //TODO
     * @Date 16:47 2020/3/4
     **/
    @ExceptionHandler(value = RuntimeException.class)//指定拦截的异常
    public void errorHandler1(HttpServletRequest request, HttpServletResponse response, Exception e) throws Exception{
        log.info("错误信息",e.getMessage());
        if (e.getMessage().equals("不允许访问")){
            String type = request.getHeader("X-Requested-With");
            if ("XMLHttpRequest".equals(type)){
                response.setContentType("application/json;charset=UTF-8");
                response.getWriter().print(JSONMessage.failure("权限不足"));
            }else{
                request.getRequestDispatcher("/pages/console/index.html").forward(request,response);
            }
        }
    }

}
