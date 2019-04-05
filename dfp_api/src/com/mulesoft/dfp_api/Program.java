package com.mulesoft.dfp_api;
//1.DFP SYNC WITH SALESFORCE
//12.REPORTS
//3.CAMPAIGN COUNT
//4.DFP DATA EXPORT
//activate_dfp_pipeline
import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;





public class Program {


	public static void main(String[] args) throws Exception {

		String[] report_type= {"EXTRACT" , "SUPPLEMENT", "SUPPLEMENT2", "FUTURE", "INVENTORY", "YIELD"};
		
		
			DFP_Manager obj = new DFP_Manager();
			sync_salesdata sync = new sync_salesdata();
			sync.sync();
			
			get_lineitems lineitems = new get_lineitems();
			lineitems.getlineitems();
			
			run_export exp = new run_export();
			exp.export();
			
			try
			{
			
				obj.RunReport("EXTRACT");
				
				
			}
			catch(Exception e) {System.out.println(e);}
		
	
	try
	{
		obj.RunReport("SUPPLEMENT");
		
	}
	catch(Exception e) {System.out.println(e);}



try
{
	obj.RunReport("SUPPLEMENT2");
	
}
catch(Exception e) {System.out.println(e);}



try
{
	obj.RunReport("FUTURE");
	
}
catch(Exception e) {System.out.println(e);}



try
{
	obj.RunReport("INVENTORY");
	
}
catch(Exception e) {System.out.println(e);}



try
{
	obj.RunReport("YIELD");
	
}
catch(Exception e) {System.out.println(e);}

run_update update = new run_update();
update.update();


UploadS3 upload = new UploadS3();

upload.cmd_upload_files();

CLIManager cmd = new CLIManager();
	   cmd.run(upload.cmd_archive_files());
		cmd.run(upload.cmd_remove_files());

activate_dfp_pipeline activate = new activate_dfp_pipeline();
activate.activate_pipeline();

		
        
	

		

}

		
	
}

