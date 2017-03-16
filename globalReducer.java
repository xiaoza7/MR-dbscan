package global;

import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Utils.*;
public class globalReducer extends Reducer<Text,Text,Text,Text>{
	
//private h;
	//private 
	Graph newGraph;
	public void setup(Context context) throws IOException, InterruptedException {//设置全局 图 结构,setup，整个task运行前之初始化一次,初始化
	    Configuration conf = context.getConfiguration();
	    newGraph=new Graph();
		 System.out.println("is begining the gloabl");
	 	 
	}
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
		Iterator<Text> ite=values.iterator();
		ArrayList<String>s=new ArrayList<String>();
		while(ite.hasNext()){
			s.add(ite.next().toString());
			
			 //StringTokenizer itr=new StringTokenizer(ite.next().toString());
			// context.write(key, new Text(ite.next()));//简单的写出,partion的id+该行数据
			// /* int id=Integer.parseInt(itr.nextToken());
			 // if(id<min) min=id;
			//  if(id>max) max=id;
			//sum++;
			
		}
		//开始插入图中
		if(s.size()==1){
			newGraph.insertsingle(s.get(0));
			
		}else if(s.size()==2){
			newGraph.seteto(s.get(0), s.get(1));
			
		}
		
		
		
	}
	 protected void cleanup(Context context
             ) throws IOException, InterruptedException {
		 //读写到
		 // 遍历性讨论
		 newGraph.trans();//已经全部遍历结束了
		 int global=0;
		 ArrayList<ArrayList<String>>results=newGraph.bianli;//暂且设为public；
		 for(int i=0;i<results.size();i++)
		 { 
			 //ArrayList<String>a=new ArrayList<String>();
			 String sline="";
			 for(int j=0;j<results.get(i).size();j++)
			 {
				sline=sline+" "+results.get(i).get(j);
			 }
			 System.out.println(sline);
			 context.write(new Text(String.valueOf(global)),new Text(sline));
			 global++;
		 }
		 System.out.println(newGraph.nodes.size());
	 	 System.out.println("is in clean up");
	 	 
		 
		//context.write(,);
		 
// NOTHING
}
 /*public void results(){
	 //输出图
	  ArrayList<Integer> finas=new ArrayList<Integer>();
	
	
	 
	 }*/
	
 
 
}
