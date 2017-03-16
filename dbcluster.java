package Utils;
import java.util.*;
import java.math.*;
public class dbcluster {
public Vector<point>dataset;
//public Vector<point>neighbors;
//public Vector<point>borders;

public  int clusterId;
public dbcluster(Vector<point>data,int partionID){
	this.dataset=data;
	
	this.clusterId=partionID;
	
}

public  double distance(point p,point q){
	//boolean isneigh=false;
	double dis=99999999956767678.0;
	if(p.size==q.size){
		// System.out.println("is in distancer");  
		double sum=0.0;
		int len=p.size;
		for(int i=0;i<len;i++)
		{
			sum+=(p.elements.get(i)-q.elements.get(i))*(p.elements.get(i)-q.elements.get(i));
		}
		// System.out.println(Math.sqrt(sum));  
		return Math.sqrt(sum);
	}
	
	return dis;
}

public Vector<point> GetNeighborhood(point p,double epi){
	int sum=0;
	Vector<point>vp=new Vector<point>();
	for(int i=0;i<this.dataset.size();i++)
	{
		point q=this.dataset.get(i);
		 //System.out.println("is in getbeighboor");  
		if(distance(p,q)<=epi){
			vp.add(q);
		}
	}
	
	return vp;
}

public void RunDbScan(int min,double epi){
	for(int i=0;i<this.dataset.size();i++)
	{
		point p=this.dataset.get(i);
		if(!p.visited){
			p.visited=true;
			Vector<point>neighbors=new Vector<point>(this.GetNeighborhood(p, epi));
			//Vector<point>neighbors=this.GetNeighborhood(p, epi);
			// System.out.println("ok");  
		//	 System.out.println(neighbors.size());
			if(neighbors.size()<min)
			  { p.isNoise=true;}
			else{
				p.clusterId=this.clusterId;
				p.iscore=true;
				for(int j=0;j<neighbors.size();j++)
				{  point p1=neighbors.get(j);
					if(!p1.visited){
						p1.visited=true;
						p1.clusterId=this.clusterId;//全局id，怎麼办,怎样使分布式的clusterID唯一,使用partition号,然后内部加1
						
					//	Vector<point>neighbors1=new Vector<point>(this.GetNeighborhood(p1, epi));//该过
						Vector<point>neighbors1=this.GetNeighborhood(p1, epi);
						System.out.println(neighbors1.size()); 
						if(neighbors1.size()>=min){
							p1.iscore=true;
						   for(int k=0;k<neighbors1.size();k++)
							{
								point sk=neighbors1.get(k);
								if(!neighbors.contains(sk)){
									neighbors.add(sk);
									
								}
							}
						//neighbors.addAll(neighbors1);
						//	System.out.println(neighbors.size()); 
							//System.out.println("clusterid is"+this.clusterId); 
							
						}else
						{
							p1.isborders=true;
						}
						
					}else if(p1.isNoise){
						
						p1.clusterId=this.clusterId;
						p1.isborders=true;
						
					}
					
				}
				
			}
		
			
		}
		this.clusterId++;//
//	System.out.println(this.clusterId); 
	System.out.println("clusterid is"+this.clusterId);	
	}
	
	
	
  }
	
}
