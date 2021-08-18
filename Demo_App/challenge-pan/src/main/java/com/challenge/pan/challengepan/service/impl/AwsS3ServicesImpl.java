package com.challenge.pan.challengepan.service.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.challenge.pan.challengepan.service.AwsS3Services;

@Service
public class AwsS3ServicesImpl implements AwsS3Services {

	private Logger logger = LoggerFactory.getLogger(AwsS3ServicesImpl.class);

	@Autowired
	private AmazonS3 s3client;

	@Override
	public void downloadObject(String bucketName, String keyName, OutputStream outputStream) {

		S3Object s3object = null;

		S3ObjectInputStream s3is = null;

		try {

			s3object = s3client.getObject(new GetObjectRequest(bucketName, keyName));

			s3is = s3object.getObjectContent();

			byte[] read_buf = new byte[1024];

			int read_len = 0;

			while ((read_len = s3is.read(read_buf)) > 0) {
				outputStream.write(read_buf, 0, read_len);
			}

			outputStream.flush();

		} catch (IOException ioe) {
			logger.error("IOException: " + ioe.getMessage());

		} catch (AmazonServiceException ase) {
			logger.info("sCaught an AmazonServiceException from GET requests, rejected reasons:");
			logger.info("Error Message:    " + ase.getMessage());
			logger.info("HTTP Status Code: " + ase.getStatusCode());
			logger.info("AWS Error Code:   " + ase.getErrorCode());
			logger.info("Error Type:       " + ase.getErrorType());
			logger.info("Request ID:       " + ase.getRequestId());
			throw ase;

		} catch (AmazonClientException ace) {
			logger.info("Caught an AmazonClientException: ");
			logger.info("Error Message: " + ace.getMessage());
			throw ace;

		} finally {

			try {
				if (s3object != null) {
					s3object.close();
					logger.info("CLOSE OBJECT DOWNLOAD KEY: " + keyName);
				}
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("ERROR CLOSE OBJECT DOWNLOAD KEY: " + keyName);
			}

			try {
				if (s3is != null) {
					s3is.close();
					logger.info("CLOSE INPUTSTREAM S3 DOWNLOAD KEY: " + keyName);
				}
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("ERROR CLOSE INPUTSTREAM S3 DOWNLOAD KEY: " + keyName);
			}

			try {
				if (outputStream != null) {
					outputStream.close();
					logger.info("CLOSE OUTPUTSTREAM S3 DOWNLOAD KEY: " + keyName);
				}
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("ERROR CLOSE OUTPUTSTREAM S3 DOWNLOAD KEY: " + keyName);
			}

		}

	}

	@Override
	public URL getPresignedUrl(String bucketName, String s3key, long durationInMinutes) {

		Date expiration = Date
				.from(LocalDateTime.now().plusMinutes(durationInMinutes).atZone(ZoneId.systemDefault()).toInstant());

		if (s3client.doesObjectExist(bucketName, s3key)) {

			return s3client.generatePresignedUrl(bucketName, s3key, expiration);

		}

		return null;
	}

	@Override
	public boolean containsObject(String bucketName, String s3Key) {

		return s3client.doesObjectExist(bucketName, s3Key);

	}

	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String s3key) {

		return s3client.getObjectMetadata(bucketName, s3key);
	}

	@Override
	public void deleteObject(String bucketName, String key) {

		s3client.deleteObject(bucketName, key);

	}

	@Override
	public void uploadObject(String bucketName, String keyName, MultipartFile multipartFile) {
		long contentLength = multipartFile.getSize();
		long partSize = 100 * 1024 * 1024;

		try {

			ObjectMetadata metadata = new ObjectMetadata();

			metadata.setContentLength(multipartFile.getSize());

			List<PartETag> partETags = new ArrayList<PartETag>();

			InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
					bucketName, keyName, metadata);

			InitiateMultipartUploadResult initResponse = s3client
					.initiateMultipartUpload(initiateMultipartUploadRequest);

			long filePosition = 0;

			for (int i = 1; filePosition < contentLength; i++) {

				partSize = Math.min(partSize, (contentLength - filePosition));

				UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(keyName)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i).withFileOffset(filePosition)
						.withInputStream(multipartFile.getInputStream()).withPartSize(partSize);

				UploadPartResult uploadResult = s3client.uploadPart(uploadRequest);

				partETags.add(uploadResult.getPartETag());

				filePosition += partSize;

				long percent = (filePosition * 100) / contentLength;

				System.out.println("Transfer.. : " + percent);

			}

			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName,
					initResponse.getUploadId(), partETags);
			s3client.completeMultipartUpload(compRequest);

		} catch (Exception ioe) {
			logger.error("IOException: " + ioe.getMessage());
		}
	}

	public List<S3ObjectSummary> listObjects(String bucketName) {

		List<S3ObjectSummary> summaries = new ArrayList<S3ObjectSummary>();

		try {

			ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);

			ListObjectsV2Result result;

			do {
				result = s3client.listObjectsV2(req);

				for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					summaries.add(objectSummary);
				}

				String token = result.getNextContinuationToken();
				req.setContinuationToken(token);

			} while (result.isTruncated());

		} catch (AmazonServiceException e) {
			e.printStackTrace();

		} catch (SdkClientException e) {
			e.printStackTrace();

		}

		if (!summaries.isEmpty()) {

			if (summaries.get(0).getKey().endsWith("/")) {

				summaries.remove(0);
			}
		}

		return summaries;

	}

}
