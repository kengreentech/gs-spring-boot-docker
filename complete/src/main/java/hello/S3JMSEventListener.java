package hello;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

@Service
public class S3JMSEventListener {

	private static final Logger LOG = LoggerFactory.getLogger(S3JMSEventListener.class);
	
	@JmsListener(destination = "test-queue")
	public void processMessage(String request) {

		LOG.info("Request: " + request);

		try {
			
			S3EventNotification s3EventNotification = S3EventNotification.parseJson(request);

			for (S3EventNotificationRecord s3EventNotificationRecord : s3EventNotification.getRecords()) {

				String sourceBucketName = s3EventNotificationRecord.getS3().getBucket().getName();
				String sourceKey = s3EventNotificationRecord.getS3().getObject().getKey();

				String destinationBucketName = "poc-test-archive";
				String targetKey = sourceKey;

				try {

					archiveFile(sourceBucketName, sourceKey, destinationBucketName, targetKey);

					updateStatus(destinationBucketName, targetKey, Status.SUCCEEDED);
					
				} catch (Exception e) {
					
					LOG.error("An exception occurred", e);

					updateStatus(destinationBucketName, targetKey, Status.FAILED);
				}
			}
			
		} catch (Exception e) { // catch-all 
			
			LOG.error("An exception occurred", e);
		}
	}

	private void archiveFile(String sourceBucketName, String sourceKey, String destinationBucketName,
			String targetKey) {
		
		LOG.info("Archiving " + sourceBucketName + "/" + sourceKey);
		
		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1).build();
		
		
		//  TODO:
		// 		- destination bucket name need to include the record stream short name
		//		- a new folder for each day should be created and the retention policy set on the folder
		
		amazonS3.copyObject(sourceBucketName, sourceKey, destinationBucketName, targetKey);

		// TODO: compare checksums

		
		
		
		LOG.info("Archiving " + sourceBucketName + "/" + sourceKey + ": COMPLETE");
	}

	private void updateStatus(String destinationBucketName, String targetKey, Status status) {
		
		LOG.info("Updating status for " + destinationBucketName + "/" + targetKey);
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
				.enableEndpointDiscovery()
				.withRegion(Regions.US_EAST_1)
				.build();

		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable("status");
		
		Map<String, Object> data = Map.of("s3BucketName", destinationBucketName, "s3Key", targetKey, "status", status.toString());
		
		PutItemOutcome putItemOutcome = table.putItem(new Item().withPrimaryKey("date", currentDateYYYYMMDD()).withMap("data", data));
		
		LOG.info("putItemOutCome: " + putItemOutcome);

	
		
		LOG.info("Updating status for " + destinationBucketName + "/" + targetKey + ": COMPLETE");
	}

	private static int currentDateYYYYMMDD() {

		return Integer.valueOf(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE));
	}
	
	private static enum Status {
		
		SUCCEEDED, FAILED
	}
}
