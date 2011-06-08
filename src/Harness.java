import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.redis.replicator.Replicator;


public class Harness{
	public static void main(String[] args) throws Exception
	{		
		try {
			Replicator replicator = new Replicator(InetAddress.getByName("127.0.0.1"), 6379);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}