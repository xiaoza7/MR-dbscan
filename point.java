package Utils;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.io.Text;
public class point implements Serializable{
 public ArrayList<Double>elements;
 public boolean iscore;
 public int id;
 public boolean visited;
 public boolean isNoise;
 public boolean isborders;
 public int partionId;
 public boolean isInner;//关键点,
 public boolean isOuter;
 public int size;
 public int clusterId;
 public point(String s){
	 elements=new ArrayList<Double>();
	 System.out.println(s); 
	 StringTokenizer itr=new StringTokenizer(s);
	 int i=0;
	 while(itr.hasMoreTokens()){
   	     String s1=itr.nextToken();
   	  //System.out.println(sline);  
   	    if(i==1){ this.id=Integer.parseInt(s1);System.out.println("is pid"+s1); }//不计算id入内
   	//判定是inner,outer点
   	    else if(i==0) {
        	    if(s1.equals("in")){
        	    	this.isInner=true;
        	    }else if(s1.equals("out")){
        	    	this.isOuter=true;
        	        }
               }
   	    else
   	    { elements.add(Double.parseDouble(s1));
   	 this.size++;
   	         }
   	      
   	     i++;
   	      
   	     
   	 // System.out.println(i);  
		 
     }
	 	 
 }
 
 public int getId(){
	 return this.id;
 }
 
}
