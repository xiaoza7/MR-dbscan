package Partition;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class partionMapper extends Mapper<LongWritable, Text, Text, Text> { 
	
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		//暂时注意,先写死数据总量，边长长度,及块大小
		
	    //String par_epsilon = conf.get("len");//get the 参数，边长，
	    //String par_outer = conf.get("outerparameter");//get the 参数，边长，
	    
	    // int total=Integer.parseInt(par_epsilon);
	     int total=150365;

	    // String parNum = conf.get("Pnum");//get the 参数，边长，
		    //double eps = Double.parseDouble(par_epsilon);
		 //int Num=Integer.parseInt(parNum);//切分的cell个数
	     int Num=10;
	    String line=value.toString();//获取数据ID，进行切分
	    int piceNum=total/Num;//块大小
	  //  float outpara=Float.parseFloat(par_outer);
	    float outpara=0.3f;
	    StringTokenizer itr=new StringTokenizer(line);
	      while(itr.hasMoreTokens()){
	    	  int id=Integer.parseInt(itr.nextToken());//获取数据ID，进行切分
	    	      int i=0;
	    	  //   while(id>i*piceNum&&id<(i+1)*piceNum&&i<piceNum) i++;//找到此id对应的块号,加入上下out,即再写入context（），改变key,即a控制范围,一列数据可以写道多个分区，
	    	       
	    	     i=id/piceNum;
	    	      
	    	     context.write(new Text(String.valueOf(i*piceNum)), new Text("in"+" "+line));//按partition号集合数据，这里partition号采用每个分区的起始数据id,为了方便localcluster,使partition唯一
	    	       //开始写外部的outer rectangle;
	    	     if(i>0){
	    	        if(id>(int)(piceNum*((i+1-outpara)))){context.write(new Text(String.valueOf((i+1)*piceNum)), new Text("out"+" "+line));}
	    	        if(id<(int)(piceNum*(i+outpara))){context.write(new Text(String.valueOf((i-1)*piceNum)), new Text("out"+" "+line)) ;}
	    	        
	    	     }else{
	    	    	 if(id>(int)(piceNum*((i+1-outpara)))){context.write(new Text(String.valueOf((i+1)*piceNum)), new Text("out"+" "+line));} 
	    	    	 
	    	     }
	    	     
	    	       //比如id<partionid+a,是outer
	      }
		
	}

}
