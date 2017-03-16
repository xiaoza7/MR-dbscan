package Partition;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class pa1 {
	public static class partionMapper1 extends Mapper<LongWritable, Text, Text, Text> { 
		
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			//暂时注意,先写死数据总量，边长长度,及块大小
			
		    //String par_epsilon = conf.get("len");//get the 参数，边长，
		    //String par_outer = conf.get("outerparameter");//get the 参数，边长，
		    
		    // int total=Integer.parseInt(par_epsilon);
		    // int total=150365;
		     int total=9831;
		    // String parNum = conf.get("Pnum");//get the 参数，边长，
			    //double eps = Double.parseDouble(par_epsilon);
			 //int Num=Integer.parseInt(parNum);//切分的cell个数
		     int Num=4;
		    String line=value.toString();//获取数据ID，进行切分
		    System.out.println(line);
		    int piceNum=total/Num;//块大小
		  //  float outpara=Float.parseFloat(par_outer);
		    float outpara=0.4f;
		    StringTokenizer itr=new StringTokenizer(line);
		   
		    	  int id=Integer.parseInt(itr.nextToken());//获取数据ID，进行切分
		    	    
		    	  int i=0;
		    	
		    	       
		    	     i=id/piceNum;
		    	      
		    	     context.write(new Text(String.valueOf(i*piceNum)), new Text("in"+" "+line));//按partition号集合数据，这里partition号采用每个分区的起始数据id,为了方便localcluster,使partition唯一
		    	       //开始写外部的outer rectangle;
		    	     if(i>0){
		    	        if(id>(int)(piceNum*((i+1-outpara)))){context.write(new Text(String.valueOf((i+1)*piceNum)), new Text("out"+" "+line));}
		    	        if(id<(int)(piceNum*(i+outpara))){context.write(new Text(String.valueOf((i-1)*piceNum)), new Text("out"+" "+line)) ;}
		    	        
		    	     }else{
		    	    	 if(id>(int)(piceNum*((i+1-outpara)))){context.write(new Text(String.valueOf((i+1)*piceNum)), new Text("out"+" "+line));} 
		    	    	 
		    	     }
		    	     System.out.println("kxj");
		    	       //比如id<partionid+a,是outer
		      }
			
	//	}

	}
	public static class partionReducer1 extends Reducer<Text,Text,Text,Text>{
		   
		public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
		    // int sum=0;int min=289343559; int max=0;
			Iterator<Text> ite=values.iterator();
			while(ite.hasNext()){
				
				 //StringTokenizer itr=new StringTokenizer(ite.next().toString());
				 context.write(key, new Text(ite.next()));//简单的写出,partion的id+该行数据
				 /* int id=Integer.parseInt(itr.nextToken());
				  if(id<min) min=id;
				  if(id>max) max=id;
				sum++;*/
			}
			//context.write(key, new Text(String.valueOf(sum)+" "+String.valueOf(min)+" "+String.valueOf(max)));//统计每一个partition的大小,及范围
		}

	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		// FIXME SETTING THE FILESYSTEM?
		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
		FileSystem dfs = FileSystem.get(conf);
		// TODO Auto-generated method stub
		 Job job = new Job(conf, "ClusterCleaner");
         job.setJarByClass(pa1.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
             
      job.setMapperClass(partionMapper1.class);
      job.setReducerClass(partionReducer1.class);
     
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
		  Path output = new Path(args[1]);
	//	if (dfs.exists(output)) dfs.delete(output, true);
      FileOutputFormat.setOutputPath(job, output);
      
          
 
			
      System.exit(job.waitForCompletion(true)?0:1);
		
	}
	}


