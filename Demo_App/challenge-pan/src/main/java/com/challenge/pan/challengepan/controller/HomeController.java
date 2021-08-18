package com.challenge.pan.challengepan.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import com.challenge.pan.challengepan.dto.DataQualityJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.challenge.pan.challengepan.service.AwsS3Services;

@Controller
public class HomeController {
	
	@Value("${datalake.bucket}")
	private String datalakeBucket;
	
	@Value("${dataquality.bucket}")
	private String dataqualityBucket;
	
	@Value("${rules.bucket}")
	private String rulesBucket;
	
	@Autowired
	AwsS3Services awsS3Services;

	@Autowired
	AWSGlue glueClient;
	
	@RequestMapping(value = "/home", method = RequestMethod.GET)
	public ModelAndView home() {
		ModelAndView mv = new ModelAndView("index");	
		mv.addObject("dataframes", awsS3Services.listObjects(datalakeBucket));
		mv.addObject("dataqualitys", awsS3Services.listObjects(dataqualityBucket));
		mv.addObject("rules", awsS3Services.listObjects(rulesBucket).stream().filter(r -> !Objects.equals(r.getKey(), "template.json")).collect(Collectors.toList()));
		mv.addObject("gluejobs", getGlueJobs());
		return mv;		
	}
	
	@RequestMapping(value = "/download", method = RequestMethod.GET)
	public void download(@RequestParam("bucket") String bucket, @RequestParam("key") String key, HttpServletResponse response) {
		ServletOutputStream outputStream = null;
		try {
			outputStream = response.getOutputStream();
		} catch (IOException e) {
			e.printStackTrace();
		}
		awsS3Services.downloadObject(bucket, key, outputStream);	
	}
	
	@RequestMapping(value = "/home", method = RequestMethod.POST)
	public String upload(@RequestParam("uploadfile") MultipartFile file) {
		awsS3Services.uploadObject(datalakeBucket, file.getOriginalFilename(), file);
		return "redirect:/home";
	}

	@RequestMapping(value = "/deleteobject")
	public String deleteObject(@RequestParam("bucket") String bucket, @RequestParam("key") String key){
			awsS3Services.deleteObject(bucket, key);
		return "redirect:/home";
	}
	@RequestMapping(value = "/rule", method = RequestMethod.POST)
	public String uploadRule(@RequestParam("uploadrule") MultipartFile file) {
		awsS3Services.deleteObject(rulesBucket, "custom_rules.json");
		awsS3Services.uploadObject(rulesBucket, "custom_rules.json", file);
		return "redirect:/home";
	}



	private List<DataQualityJob> getGlueJobs(){
		List<DataQualityJob> dataQualityJobs = new ArrayList<>();
		GetJobRunsRequest runsRequest = new GetJobRunsRequest();
		runsRequest.setJobName("DQ_FIAP_PAN");
		runsRequest.setMaxResults(10);
		GetJobRunsResult jobRuns = glueClient.getJobRuns(runsRequest);
		List<JobRun> runs = jobRuns.getJobRuns();
		runs.forEach(jrun -> dataQualityJobs.add(new DataQualityJob(jrun)));
		return dataQualityJobs;
	}

}
