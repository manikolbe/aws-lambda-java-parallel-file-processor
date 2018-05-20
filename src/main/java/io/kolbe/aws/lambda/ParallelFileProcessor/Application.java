package io.kolbe.aws.lambda.ParallelFileProcessor;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;


public class Application implements RequestHandler<S3Event, String> {

    /**
     * Example implementation for processing an S3 event. This is the method which gets invoked by lambda
     * when an object is created in S3.
     *
     * @param s3Event the event object.
     * @param context the AWS lambda context object.
     * @return a response code.
     */
    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);

            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            System.out.println("Processing S3 object" + srcBucket + "/" + srcKey);

            // Download the image from S3 into a stream
            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            InputStream objectData = s3Object.getObjectContent();
            printLines(objectData);
            return "Ok";

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Received an inputStream and print each line
     * @param input
     * @throws IOException
     */
    private static void printLines(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        System.out.println("#### Starting to print file");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println("#### Finished printing file");
    }

}
