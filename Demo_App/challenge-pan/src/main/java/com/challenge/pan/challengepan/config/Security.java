package com.challenge.pan.challengepan.config;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
@EnableWebSecurity
public class Security extends WebSecurityConfigurerAdapter{
    @Override
    protected void configure(HttpSecurity http) throws Exception { 
        http
        .cors().and()
        .csrf().disable().authorizeRequests()
        .antMatchers("/users").hasRole("manager")        
        .anyRequest().authenticated()
        .and()
        .formLogin()
        .defaultSuccessUrl("/home", true)
        .and().logout().permitAll();
    }
    
	@Override
	public void configure(WebSecurity web) throws Exception {
		web.ignoring().antMatchers("/js/**");
		web.ignoring().antMatchers("/css/**");		
		web.ignoring().antMatchers("/img/**");
		web.ignoring().antMatchers("/video/**");
		web.ignoring().antMatchers("/webjars/**");
	}

}
