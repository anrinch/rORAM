package pathORAM;

public class ClientQueue {

	private static ClientQueue queue;
	
	private int head;
	private int tail;
	private int[] q;
	private int num_clients;
	
	
	public static ClientQueue getInstance(int num_clients){
		if (queue == null){
			queue = new ClientQueue(num_clients);
		}
		return queue;
	}
	
	private ClientQueue(int num_clients){
		q = new int[num_clients];
		head = 0;
		tail = 0;
		this.num_clients = num_clients;
	}
	
	public void push(int client_id){
		q[head] = client_id;
		head = (head + 1) % this.num_clients;
		//if (head == num_clients)
			//head =0;
			
	}
	
	public void pop(){
		
		
	//	if (tail+1 == num_clients)
			
		//	tail = 0;
			
	//	else
				
			tail = (tail+1) % this.num_clients;
	}
	
	public int getTop(){
		return q[tail];
	}
	
	public int[] getQ(){
		return q;
	}
	
	public void reset(){
		head = 0;
		tail = 0;
	}
	
	public boolean isFull(){
		if (head== num_clients)
			return true;
		return false;
		
	}
	
	public int getHead(){
		return head;
	}
	public int getTail(){
		return tail;
	}
}
