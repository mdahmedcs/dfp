package com.mulesoft.dfp_api;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimestampManager {

	public TimestampManager() {		
	}
	
	public String getTimeStamp() 
	{		
		DateFormat df = null;
		Date today = null;
		String fileDate = "";
		
		try {
			// Create an instance of SimpleDateFormat used for formatting 
			// the string representation of date (month/day/year)
			df = new SimpleDateFormat("yyyy-MM-dd-HHmmss");

			// Get the date today using Calendar object.
			today = Calendar.getInstance().getTime();        
			
			// Using DateFormat format method we can create a string 
			// representation of a date with the defined format.
			fileDate = df.format(today);			
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in getTimeStamp: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
		}
		
		return fileDate;
	}	
	
	public String getTimeStampFromFilename(String filename) 
	{		
		
		String[] filenameParts = null;
		String fileDate = "";
		
		try {			
   			filenameParts = filename.split("_");
   			fileDate = filenameParts[1].replace(".tar.gz", "");					
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in getTimeStamp: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
		}
		
		return fileDate;
	}		
}
