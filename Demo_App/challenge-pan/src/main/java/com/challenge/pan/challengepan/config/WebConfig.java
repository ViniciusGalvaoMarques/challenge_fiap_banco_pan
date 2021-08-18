package com.challenge.pan.challengepan.config;

import javax.servlet.MultipartConfigElement;

import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.unit.DataSize;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@EnableAsync
@EnableScheduling
@EnableWebMvc
@Configuration
public class WebConfig implements WebMvcConfigurer {

	@Bean
	public MultipartConfigElement multipartConfigElement() {
		MultipartConfigFactory factory = new MultipartConfigFactory();
		factory.setMaxFileSize(DataSize.ofGigabytes(50L));
		factory.setMaxRequestSize(DataSize.ofGigabytes(50L));
		return factory.createMultipartConfig();
	}
	
	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry
        .addResourceHandler("/**", "/webjars/**")
        .addResourceLocations("classpath:/static/", "/webjars/");     
	}

	
}
