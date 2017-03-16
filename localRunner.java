package localcluster;

import java.io.IOException;

import localcluster.localrunner1.localMapper1;
import localcluster.localrunner1.localReducer1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Partition.partionMapper;
import Partition.partionReducer;

public class localRunner {
       public localRunner() {}
	
	public void run(String input_file, String output_file) throws IOException {
		
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		 Job job = new Job(conf, "Clusterlocal");
         job.setJarByClass(localRunner.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
             
      job.setMapperClass(localMapper.class);
     // job.setCombinerClass(localReducer1.class);不能设combinner， 数据写到
      job.setReducerClass(localReducer2.class);
     
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
     // FileInputFormat.addInputPath(job, new Path(args[0]));
		//  Path output = new Path(args[1]);
	//	if (dfs.exists(output)) dfs.delete(output, true);
    //  FileOutputFormat.setOutputPath(job, new Path(args[1]));
      FileInputFormat.addInputPath(job, new Path(input_file));
      Path output = new Path(output_file);
  	//	if (dfs.exists(output)) dfs.delete(output, true);
          FileOutputFormat.setOutputPath(job, output);
			
       //System.exit(job.waitForCompletion(true)?0:1);
		
		
       /* Job job = new Job(conf, "localCluster");
           
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
               
        job.setMapperClass(localMapper.class);
        job.setReducerClass(localReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input_file));
		  Path output = new Path(output_file);
	//	if (dfs.exists(output)) dfs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);*/
            
        try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
		
	}
	
	
	
	
	

