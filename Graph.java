package Utils;

import java.util.ArrayList;
import java.util.HashMap;

public class Graph {
	
	
	
	
	public ArrayList<Graphnode>nodes;
	public ArrayList<ArrayList<String>>bianli;
	public Graph(){
	   nodes=new ArrayList<Graphnode>();
		bianli=new ArrayList<ArrayList<String>>();
	}
	
	
	
	
	
	public boolean ItermExists(String id){
		boolean b=false;
		Graphnode p;
		
		if(!nodes.isEmpty()){
			for(int i=0;i<nodes.size();i++){
				p=nodes.get(i);
				if(p.beginid.equals(id)){
					b=true;
					break;
				}
			}
		}
		
		return b;
	}
	public void insertsingle(String id){
		if(!ItermExists(id)){
			Graphnode p=new Graphnode(id);
			nodes.add(p);	
		}
		
	}
	public Graphnode find(String id){
		
		Graphnode p=null;
		
		if(!nodes.isEmpty()){
			for(int i=0;i<nodes.size();i++){
				p=nodes.get(i);
				if(p.beginid.equals(id)){
					return p;
					//break;
				}
			}
		}
		
		return p;
	}
	public void test(String begin,String end){
		if(!ItermExists(begin)){
			Graphnode p=new Graphnode(begin);
			nodes.add(p);	
		}
		if(!ItermExists(end)){
			Graphnode p=new Graphnode(end);
			nodes.add(p);	
		}
		
		
	}
	public boolean insertest(Graphnode a,Graphnode b){
		boolean bs=false;
	//	Graphnode p=a.next;
		Graphnode p=a;
		while(p!=null){
			
			if(p.beginid.equals(b.beginid)){
				bs=true;break;
			}
			p=p.next;
		}
		return bs;
		
	}
	public void seteto(String beginid,String endid){
		test(beginid,endid);
		Graphnode p=find(beginid);
		Graphnode q=find(endid);
		if(p!=null){
			if(!insertest(p,q)){
				Graphnode p1=new Graphnode(endid);
				p1.next=p.next;
				p.next=p1;
			}
		}
		if(q!=null){
			if(!insertest(q,p)){
				Graphnode p1=new Graphnode(beginid);
				p1.next=q.next;
				q.next=p1;
			}
			
		}
		
		
		
	}
	public void addTrans(Graphnode pa,ArrayList<String>s){
		if(!pa.visited){
			 //tempList=new ArrayList<String>();
			pa.visited=true;
			s.add(pa.beginid);
			while(pa.next!=null){
			//	Graphnode p=pa.next;
			   Graphnode pb=find((pa.next).beginid);
				//s.add(p.beginid);
				addTrans(pb,s);
				pa=pa.next;
				
			}
			
			
		}
		
		
	}
	
	public void trans(){  //深度遍历
		
		for(int i=0;i<nodes.size();i++){
			Graphnode p=nodes.get(i);
			ArrayList<String> tempList=new ArrayList<String>();
			if(!p.visited){
				//p.visited=true;
				addTrans(p,tempList);
				
			}
			if(tempList.size()>0)//添加子图
			{bianli.add(tempList);}
		}
			
	}

}
