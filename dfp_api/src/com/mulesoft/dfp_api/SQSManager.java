package com.mulesoft.dfp_api;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSManager {
	public void SQSManager() { 	
    }
    
    public void sendMessage(String msg)
    {
    	try {
    		String sqsAccessKey = SupportFuncs.getProperties().getProperty("sqs_access_key");
    		String sqsSecretKey = SupportFuncs.getProperties().getProperty("sqs_secret_key");	
    		String subFormQueue = SupportFuncs.getProperties().getProperty("sqs_queue_name");
    		BasicAWSCredentials awsCreds = new BasicAWSCredentials(sqsAccessKey, sqsSecretKey);
        	AmazonSQS sqs = new AmazonSQSClient(awsCreds);
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            sqs.setRegion(usEast1);
            String subFormQueueURL = "";
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                if (queueUrl.contains(subFormQueue)) {
                	subFormQueueURL = queueUrl;
                	break;
                }                	
            }    		
            sqs.sendMessage(new SendMessageRequest(subFormQueueURL, msg));
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in sendMessage: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}
    }
}
