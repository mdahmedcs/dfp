package com.mulesoft.dfp_api;

import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;

public class CLIManager {

	public CLIManager() {
		
	}
	
    int iExitValue;
    String sCommandString;

	public void run(String awsScript) {

		
		try {
			runScript(awsScript);
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in CLIManager.run: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
		}		
	}
	
	private void runScript(String command){
		
        sCommandString = command;
        CommandLine oCmdLine = CommandLine.parse(sCommandString);
        DefaultExecutor oDefaultExecutor = new DefaultExecutor();
        oDefaultExecutor.setExitValue(0);
        try {
            iExitValue = oDefaultExecutor.execute(oCmdLine);
        } catch (ExecuteException e) {
 	   		String exceptionMsg = "Execution failed in CLIManager.runScript: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
        } catch (IOException e) {
 	   		String exceptionMsg = "Permission denied occurred in CLIManager.runScript: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
        }
    }
}
