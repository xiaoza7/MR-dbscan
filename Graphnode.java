package Utils;



public class Graphnode {
	
		public String beginid;
	//	String endid;
		public Graphnode next;
		boolean visited;
	
	public Graphnode(String id){
		this.beginid=id;
		this.next=null;
		this.visited=false;
		 System.out.println("is in node");
	 	 
	}

}
