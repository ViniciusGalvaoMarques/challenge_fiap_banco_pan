package com.challenge.pan.challengepan.service;

import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public interface AwsS3Services {
	
	public URL getPresignedUrl (String bucketName, String s3key, long durationInMinutes);
	
	public void downloadObject(String bucketName, String keyName, OutputStream outputStream);	
	
	public void uploadObject(String bucketName, String keyName, MultipartFile file);
	
	public List<S3ObjectSummary> listObjects(String bucketName);
	
	public boolean containsObject(String bucketName, String s3Key);
	
	public ObjectMetadata getObjectMetadata(String bucketName, String s3key);
	
	public void deleteObject(String bucketName, String key);
	
	
}