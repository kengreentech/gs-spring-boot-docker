package hello;

import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.amazonaws.services.s3.event.S3EventNotification;

public class S3NotificationTest {

	@Test
	public void parse() throws Exception {

		S3EventNotification s3EventNotification = S3EventNotification.parseJson(FileUtils
				.readFileToString(new ClassPathResource("s3-event-notification.json").getFile(), StandardCharsets.UTF_8));
		
		System.out.println(s3EventNotification);
	}
}
