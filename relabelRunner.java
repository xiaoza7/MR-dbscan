package relabel;

import localcluster.localrunner1;
import localcluster.localrunner1.localMapper1;
import localcluster.localrunner1.localReducer1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class relabelRunner {

	public static void main(String[] args)  throws Exception{
		// TODO Auto-generated method stub
        
		Configuration conf = new Configuration();
   		// FIXME SETTING THE FILESYSTEM?
   		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
   		//FileSystem dfs = FileSystem.get(conf);
   		// TODO Auto-generated method stub
   		 Job job = new Job(conf, "relabellocal");
            job.setJarByClass(relabelRunner.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
                
         job.setMapperClass(relabelMapper.class);
        // job.setCombinerClass(localReducer1.class);不能设combinner， 数据写到
         job.setReducerClass(relabelReducer.class);
        
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         
         FileInputFormat.addInputPath(job, new Path(args[0]));
   		  Path output = new Path(args[1]);
   	//	if (dfs.exists(output)) dfs.delete(output, true);
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         

   			
         System.exit(job.waitForCompletion(true)?0:1);
	}

}
