package data;

import java.io.Serializable;

public class DataBlock extends DataItem implements Serializable {

		private long Id;
		private byte[] payload;
		
		public DataBlock(){
			this.Id = 0;
			this.payload = new byte[4096];
		}
		
		public DataBlock(long ID, byte[] b) {
			this.Id = ID;
			payload = b;
				
		}
		
		
		
		public long get_id(){
			return this.Id;
		}
		
		public void set_id(long Id){
			this.Id = Id;
		}
		
		public byte[] get_payload(){
			return this.payload;
		}
		
		public void set_payload(byte[] payload){
			this.payload = payload;
		}

		@Override
		public byte[] getData() {
			return this.payload;
		}
	}


