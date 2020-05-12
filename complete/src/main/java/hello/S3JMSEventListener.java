package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetObjectRetentionRequest;

@Service
public class S3JMSEventListener {

	private static final Logger LOG = LoggerFactory.getLogger(S3JMSEventListener.class);
	
	@JmsListener(destination = "test-queue")
	public void processMessage(String request) {

		LOG.info("Request: " + request);

		S3EventNotification s3EventNotification = S3EventNotification.parseJson(request);

		for (S3EventNotificationRecord s3EventNotificationRecord : s3EventNotification.getRecords()) {

			AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
					.withCredentials(new ProfileCredentialsProvider())
					.withRegion(Regions.DEFAULT_REGION).build();
			
			String sourceBucketName = s3EventNotificationRecord.getS3().getBucket().getName();
			String sourceKey = s3EventNotificationRecord.getS3().getObject().getKey();
			
			//  TODO: destination bucket name need to include the record stream short name 
			String destinationBucketName = "poc-archive-target";
			String targetKey = sourceKey;
			
			amazonS3.copyObject(sourceBucketName, sourceKey, destinationBucketName, targetKey);

			// TODO: compare checksums
			
			// TODO: write records to DynamoDB

			
			
		}
		
		
	}
}
