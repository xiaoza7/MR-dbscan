package localcluster;


	
	
	import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

	import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Partition.pa1;
	import Partition.partionMapper;
import Partition.partionReducer;
import Partition.pa1.partionMapper1;
import Partition.pa1.partionReducer1;
import Utils.dbcluster;
import Utils.point;

	public class localrunner1 {
		public static class localMapper1 extends Mapper<LongWritable,Text,IntWritable,Text>{
			public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
				String line=value.toString();//获取数据块ID
			 //  System.out.println("is in map");
			    StringTokenizer itr=new StringTokenizer(line);
			    String id=itr.nextToken();
			    String sline="";
			     while(itr.hasMoreTokens()){
			    	 // int id=Integer.parseInt(itr.nextToken());//获取数据ID，进行切分
			    	      //int i=0;
			    	 sline=sline+" "+itr.nextToken();
			    	 
			     }
			    
			    	    // context.write(new Text(id), new Text(sline));//按partion号集合数据,包含inner,outer rectangle;
			    	     context.write(new IntWritable(Integer.parseInt(id)), new Text(sline));
			    	       
			     //	System.out.println(id+sline);  
				
				
				
			}
			
			

		}
		public static class  localReducer1 extends Reducer<IntWritable,Text,IntWritable,Text>{
			//public  Vector<point>set;
			public void reduce(IntWritable key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
				
				
				Iterator<Text> ite=values.iterator();
				Vector<point>set=new Vector<point>();
				
				while(ite.hasNext()){
					String line=ite.next().toString();
				//	System.out.println(line);  
					point p=new point(line);
					p.partionId=Integer.parseInt(key.toString());
					
					set.add(p);
					
				}
			// System.out.println(set.size());  
				//开始本地聚类
				dbcluster maindb=new dbcluster(set,Integer.parseInt(key.toString()));
				maindb.RunDbScan(12, 0.05);//参数或许可以修改，reduce 可以读取context获取全局信息,暂且写死。
				String writecontent;
				
				//多类型数据输出同一个文件，以类型区别
				for(int i=0;i<set.size();i++){
					String pointflags = null;
					point p=set.get(i);
					//if(p.iscore&&!p.isNoise)
					if(p.iscore)
					  {pointflags="core";}
					else if(p.isborders){
						pointflags="border";
						
					}else{
						pointflags="Noise";
					}
					//首先是无论什么标志,均写出
					String pclusid=String.valueOf(p.clusterId);
					writecontent=pclusid+" "+pointflags;//poin的clusterid+p.flag;
				//	 System.out.println(writecontent);  
				   context.write(new IntWritable(p.id),new Text(writecontent) );//key:p.id, value:poin的clusterid+p.flag;
					//判定inner或者outer,再次写出，为合并各个localcluster做好准备
					if(p.iscore&&p.isInner){
						context.write(new IntWritable(p.id),new Text("incore"+" "+writecontent) );	
					}
					else if(!p.isNoise&&p.isOuter){
						context.write(new IntWritable(p.id),new Text("bordercore"+" "+writecontent) );	
						
					}
					
					//上面的都写出到同一个文件中去了,长度不同，读取时注意
					
				}
				
			}
			
			

		}

		
	      // public localrunner1() {}
	       
	       public static void main(String[] args) throws Exception{
	   		Configuration conf = new Configuration();
	   		// FIXME SETTING THE FILESYSTEM?
	   		//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
	   		//FileSystem dfs = FileSystem.get(conf);
	   		// TODO Auto-generated method stub
	   		 Job job = new Job(conf, "Clusterlocal");
	            job.setJarByClass(localrunner1.class);
	         job.setOutputKeyClass(IntWritable.class);
	         job.setOutputValueClass(Text.class);
	                
	         job.setMapperClass(localMapper1.class);
	        // job.setCombinerClass(localReducer1.class);不能设combinner， 数据写到
	         job.setReducerClass(localReducer1.class);
	        
	         job.setInputFormatClass(TextInputFormat.class);
	         job.setOutputFormatClass(TextOutputFormat.class);
	         
	         FileInputFormat.addInputPath(job, new Path(args[0]));
	   		  Path output = new Path(args[1]);
	   	//	if (dfs.exists(output)) dfs.delete(output, true);
	         FileOutputFormat.setOutputPath(job, new Path(args[1]));
	         

	   			
	         System.exit(job.waitForCompletion(true)?0:1);
	   		
	   	}
		/*
		public void run(String input_file, String output_file) throws IOException {
			
			Configuration conf = new Configuration();
			// FIXME SETTING THE FILESYSTEM?
			//conf.set("fs.default.name","hdfs://127.0.0.1:54310/");		
			FileSystem dfs = FileSystem.get(conf);
			
	        Job job = new Job(conf, "localCluster");
	           
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	               
	        job.setMapperClass(localMapper.class);
	        job.setReducerClass(localReducer.class);
	       
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        
	        FileInputFormat.addInputPath(job, new Path(input_file));
			  Path output = new Path(output_file);
		//	if (dfs.exists(output)) dfs.delete(output, true);
	        FileOutputFormat.setOutputPath(job, output);
	            
	        try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
			
			
		}
		


