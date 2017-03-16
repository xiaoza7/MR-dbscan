package main;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import global.globalRunner;

import Partition.PartitionRunner;
import localcluster.localRunner;
public class dbScanMain {
	private String input_file;
	private String output_file;
	
	private void runScan() throws IOException {
		Path p = new Path(this.input_file);
		String partion_file = 
				new String(p.getParent().toString()+"/partion.txt");
		//第一部分
		PartitionRunner pr=new PartitionRunner();
		pr.run(this.input_file, partion_file);
		//第二部分
		
		String localcluster_file = 
				new String(p.getParent().toString()+"/localcluster.txt");
		//第一部分
		localRunner lr=new localRunner();
		lr.run(partion_file, localcluster_file);
		
		
		//第三部分
	//	globalRunner gr=new globalRunner();
	
	}
	public dbScanMain(String input, String output) {
		this.input_file = input;
		this.output_file = output;
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
    
		String input_file = args[0];
		String output_file = args[1];
		long startTime = System.currentTimeMillis();		
		// First, obtain the entropy order of the attributes
		
		Path p = new Path(output_file);
	
		long endTime = System.currentTimeMillis();
		String times = new String();
		times = input_file + " " + args[0] + " " + args[1] + 
				" " + String.valueOf(endTime - startTime);
		// Run scan with the order found
		dbScanMain s = new dbScanMain(input_file, output_file);
		//Path p = new Path(s.input_file);
		String partion_file = 
				new String(p.getParent().toString()+"/partion.txt");
		//第一部分
		PartitionRunner pr=new PartitionRunner();
		try {
			pr.run(input_file, partion_file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
