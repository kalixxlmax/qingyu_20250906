package com.basic.im.security.config;


import com.alibaba.fastjson.JSON;
import com.basic.im.comm.utils.LoginPassword;
import com.basic.im.security.handle.CustomAccessDeniedHandler;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * @Description: TODO （Spring Security 配置类）
 * @Author xie yuan yang
 * @Date 2020/3/3
 **/

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true,securedEnabled=true,jsr250Enabled=true)
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    //用户查询服务组件接口
    @Autowired
    private UserDetailsServiceImpl userDetailsService;

    @Autowired
    private CustomAccessDeniedHandler customAccessDeniedHandler;

    /**
     * @Description //TODO 解决Spring Security 高版本对URL校验更加严格对 //放行进行处理
     * @Date 16:28 2020/3/20
     **/
    /*@Bean
    public HttpFirewall allowUrlEncodedSlashHttpFirewall() {
        StrictHttpFirewall firewall = new StrictHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        return firewall;
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        super.configure(web);
        web.httpFirewall(allowUrlEncodedSlashHttpFirewall());
    }*/

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        //校验密码器
        auth.userDetailsService(userDetailsService).passwordEncoder(
                new PasswordEncoder() {
                    @Override
                    public String encode(CharSequence rawPassword) {
                        return rawPassword.toString();
                    }

                    @Override
                    public boolean matches(CharSequence rawPassword, String encodedPassword) {
                       return encodedPassword.equals(rawPassword)||encodedPassword.equals(LoginPassword.encodeFromOldPassword(rawPassword.toString()));
                    }
                });
    }

    /**
     * @Description //TODO (开启注解)
     * @Date 12:56 2020/3/3
     **/
    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }


    /**
     * @Description //TODO (控制对权限的访问)
     * @Date 11:57 2020/3/3
     **/
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        //解决 in a frame because it set 'X-Frame-Options' to 'deny'.
        http.headers().frameOptions().disable();

        ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry authorizeRequests = http.authorizeRequests();
        //设置任何人可以访问的资源
        authorizeRequests.antMatchers("/**").permitAll();
        //设置 1000 是最高管理员 console资源随便访问。
      /* authorizeRequests.antMatchers("/console/**").hasAnyRole("1000");*/
        //剩下的资源都要权限验证
        authorizeRequests.anyRequest().authenticated();

        //警用CSRF
        http.csrf().disable();

        //登录表单操作
        http.formLogin()
                //登录资源路径
                .loginProcessingUrl("/console/login")
                //账号属性名
                .usernameParameter("account")
                //密码属性名
                .passwordParameter("password")
                //表单登陆成功
                .successHandler(new SavedRequestAwareAuthenticationSuccessHandler(){
                    @Override
                    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) throws ServletException, IOException {
                        String account = request.getParameter("account");
                        Map<String,Object> data = JSON.parseObject(SKBeanUtils.getRedisCRUD().get(account + "_loginData"),Map.class);

                        String type = request.getHeader("X-Requested-With");
                        if ("XMLHttpRequest".equals(type)){
                            response.setContentType("application/json;charset=UTF-8");
                            response.getWriter().print(JSONMessage.success(data));
                        }else{
                            request.getRequestDispatcher("/pages/console/index.html").forward(request,response);
                        }
                    }
                })
                //表单登陆失败
                .failureHandler(new AuthenticationFailureHandler() {
                    @Override
                    public void onAuthenticationFailure(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, AuthenticationException e) throws IOException, ServletException {
                        System.out.println(e.getMessage());
                        String type = httpServletRequest.getHeader("X-Requested-With");
                        if ("XMLHttpRequest".equals(type)){
                            httpServletResponse.setContentType("application/json;charset=UTF-8");
                            if ("Bad credentials".equals(e.getMessage())){
                                httpServletResponse.getWriter().print(JSONMessage.failure("账号或密码错误!"));
                            }else{
                                httpServletResponse.getWriter().print(JSONMessage.failure(e.getMessage()));
                            }
                        }else{
                            httpServletRequest.getRequestDispatcher("/console/login").forward(httpServletRequest,httpServletResponse);
                        }
                    }
                });


                //注销操作
                http.logout()
                        .logoutUrl("/console/logout");
                        //.logoutSuccessUrl("/console/login");


                //异常处理
                http.exceptionHandling().accessDeniedHandler(customAccessDeniedHandler);
    }

    @Bean
    public HttpFirewall defaultHttpFirewall() {
        return new DefaultHttpFirewall();
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
    //新防火墙强制覆盖原来的
        web.httpFirewall(defaultHttpFirewall());
        //super.configure(web);
    }
}
