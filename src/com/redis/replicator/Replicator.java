package com.redis.replicator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class Replicator {
	
	Boolean isFinishedBulkLoad = false;
	HashMap<String, String> keys;
	Socket socket;
	
	public Replicator(final InetAddress host,final int port){
		keys = new HashMap<String, String>();
		
		try {
			
			this.socket = new Socket(host, port);
			
			Thread thread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						connect();
						
						while(socket.isConnected()){
							InputStream in = socket.getInputStream();
							String line = readLine(in);
							if(line.charAt(0) == '*'){
								Integer.parseInt(line.substring(1).trim());
								line = readLine(in);
								int command_length = Integer.parseInt(line.substring(1).trim());
								String command = readFor(in, command_length);
								if(!command.equals("set")) break;
								skipBytes(in, 2);
								line = readLine(in);
								int keyLength = Integer.parseInt(line.substring(1).trim());
								String key = readFor(in, keyLength);
								skipBytes(in, 2);
								line = readLine(in);
								int valueLength = Integer.parseInt(line.substring(1).trim());
								String value = readFor(in, valueLength);
								skipBytes(in, 2);	
								keys.put(key, value);
								System.out.println("Key: " + key + " Value: " + value);
							}
						}
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}
			}, "replicator");
			
			thread.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String readFor(InputStream in, int length) throws IOException{
		StringBuilder strb = new StringBuilder();
		
		for(int i = 0; i < length; i++){
			strb.append((char)in.read());
		}
		return strb.toString();
	}
	
	private void skipBytes(InputStream in, int length) throws IOException{
		for(int i = 0; i < 2; i++) in.read();
	}
	
	private String readLine(InputStream in) throws IOException{
		int byteRead = 0;
		StringBuilder strb = new StringBuilder();
		//Read Line
		while((byteRead = in.read()) >= 0 && (char)byteRead != '\n'){
			strb.append((char)byteRead);
		}
		
		return strb.toString();
	}
	
	private void connect() throws Exception
	{
		InputStream in = socket.getInputStream();
		OutputStream out = socket.getOutputStream();
		out.write("SYNC\r\n".getBytes());
		int readByte;
		int bufferPosition = 0;
		
		StringBuilder strBuilder = new StringBuilder();
		
		do{
			readByte = in.read();
			strBuilder.append((char)readByte);
			bufferPosition++;
		}while(readByte != (int)'\n' || bufferPosition <= 1);
		
		String lengthStr = strBuilder.toString().trim().substring(1);
		int bodyLength = Integer.parseInt(lengthStr);
		
		ByteBuffer rdbData = ByteBuffer.allocate(bodyLength);
		int bytePosition = 0;
		
		do{
			rdbData.put((byte)in.read());
			bytePosition++;
		}while(bytePosition < bodyLength);
		
		System.out.println(strBuilder.toString());
		
		rdbData.rewind();
		
		RDB rdb = new RDB();
		rdb.rdb_load(rdbData, new Visitor() {
			
			@Override
			public void callback(String key, String value) {
				System.out.println("Key: " + key + " Value: " + value);				
			}
		});
	}	
}
