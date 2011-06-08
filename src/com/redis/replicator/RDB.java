package com.redis.replicator;

import java.nio.ByteBuffer;

import com.ning.compress.lzf.LZFDecoder;

public class RDB {

	final int REDIS_EXPIRETIME = 253;
	final int REDIS_SELECTDB = 254;
	final int REDIS_EOF = 255;

	final int REDIS_STRING = 0;
	final int REDIS_LIST = 1;
	final int REDIS_SET = 2;
	final int REDIS_ZSET = 3;
	final int REDIS_HASH = 4;
	
	final int REDIS_HASH_ZIPMAP = 9;
	final int REDIS_LIST_ZIPLIST = 10;
	final int REDIS_SET_INTSET = 11;
	final int REDIS_ZSET_ZIPLIST = 12;
	
	final int REDIS_RDB_6BITLEN = 0;
	final int REDIS_RDB_14BITLEN = 1;
	final int REDIS_RDB_32BITLEN = 2;
	final int REDIS_RDB_ENCVAL = 3;
	
	final int REDIS_RDB_ENC_INT8 = 0;
	final int REDIS_RDB_ENV_INT16 = 1;
	final int REDIS_RDB_ENV_INT32 = 2;
	final int REDIS_RDB_ENV_LZF = 3;
	
	final double R_POST_INF = 1.0f;
	final double R_NEG_INF = -1.0f;
	final double R_NAN = 0.0f;
	
	public Object[] rdb_load_length(ByteBuffer file){
		Boolean isEncoded = false;
		
		byte buffer = file.get();
		byte type = (byte) ((buffer & 0xC0) >> 6);
		
		switch(type)
		{
		case REDIS_RDB_6BITLEN:
			return new Object[]{isEncoded,buffer & 0x3F};
		case REDIS_RDB_ENCVAL:
			isEncoded = true;
			return new Object[]{isEncoded,buffer & 0x3F};
		case REDIS_RDB_14BITLEN:
			 return new Object[]{isEncoded, ((buffer & 0x3f) << 8) | file.get()};
		default:
			int retVal = 0;
			for(int i = 0; i < 4; i++) retVal = retVal << 8 | file.get();
			return new Object[]{isEncoded, retVal};
		}
	}
	
	public String rdbLoadIntegerObject(ByteBuffer file, Integer encoding_type, boolean encode) throws Exception{
		String retVal = null;
		byte[] bytes;
		switch(encoding_type)
		{
			case REDIS_RDB_ENC_INT8:
				retVal = new String(new byte[]{file.get()});
				break;
			case REDIS_RDB_ENV_INT16:
				bytes = new byte[2];
				for(int i =0 ; i < 2; i++) bytes[i] = file.get();
				retVal = new String(bytes);
				break;
			case REDIS_RDB_ENV_INT32:
				bytes = new byte[4];
				for(int i = 0; i < 4; i++) bytes[i] = file.get();
				retVal = new String(bytes);
				break;
			default:
				throw new Exception("Unknown RDB integer encoding type");
		}
		return retVal;
	}
	
	public int rdbLoadType(ByteBuffer file){ 
		return file.get()&0xFF;
	}
	
	public String rdbLoadTime(ByteBuffer file){
		byte[] bytes = new byte[4];
		for(int i = 0; i < 4; i++) bytes[i] = file.get();		
		return new String(bytes);		
	}
	
	public String rdbLoadLzfStringObject(ByteBuffer file){
		try{
			Integer compressed_length = (Integer)rdb_load_length(file)[1];
			Integer length = (Integer)rdb_load_length(file)[1];
			byte[] buffer = new byte[compressed_length];
			byte[] output = new byte[length];
			
			output = LZFDecoder.decode(buffer);
			
			return new String(output);
			
		}catch(Exception ex){
			return null;
		}
	}
	
	public String rdbGenericLoadStringObject(ByteBuffer file, boolean encode) throws Exception{
		Object[] loadLength = rdb_load_length(file);
		Boolean isEncoded = (Boolean) loadLength[0];
		Integer length = (Integer)loadLength[1];
		
		if(isEncoded){
			switch(length){
			case REDIS_RDB_ENC_INT8:
			case REDIS_RDB_ENV_INT16:
			case REDIS_RDB_ENV_INT32:
				return rdbLoadIntegerObject(file, length, encode);
			case REDIS_RDB_ENV_LZF:
				return rdbLoadLzfStringObject(file);
			default:
				throw new Exception("Unknown RDB encoding type");				
			}
		}
		
		byte[] bytes = new byte[length];
		for(int i = 0; i < length; i++) bytes[i] = file.get();
		
		return new String(bytes);
		
	}
	
	public String rdbLoadStringObject(ByteBuffer file) throws Exception{
		return rdbGenericLoadStringObject(file, false);
	}
	
	public String rdbLoadEncodedStringObject(ByteBuffer file) throws Exception{
		return rdbGenericLoadStringObject(file, true);
	}
	
	public Double rdbLoadDoubleValue(ByteBuffer file){
		int length = file.get();
		switch(length){
		case 255:
			return R_NEG_INF;
		case 254:
			return R_POST_INF;
		case 253:
			return R_NAN;
		default:
			byte[] buffer = new byte[length];
			for(int i = 0; i < length; i++) buffer[i] = file.get();
			String str = new String(buffer);
			return Double.valueOf(str);			
		}
	}
	
	public String rdbLoadObject(int type, ByteBuffer file) throws Exception{
		Object[] loadLength;
		Integer length;
		
		switch(type){
		case REDIS_STRING:
			return rdbLoadEncodedStringObject(file);
		case REDIS_LIST:
		case REDIS_SET:
			loadLength = rdb_load_length(file);
			length = (Integer) loadLength[1];
			for(int i = 0; i < length; i++) rdbLoadEncodedStringObject(file);
			return null;
		case REDIS_ZSET:
			loadLength = rdb_load_length(file);
			length = (Integer) loadLength[1];
			for(int i = 0; i < length; i++){
				rdbLoadEncodedStringObject(file);
				rdbLoadDoubleValue(file);
			}
			return null;
		case REDIS_HASH:
			loadLength = rdb_load_length(file);
			length = (Integer)loadLength[1];
			for(int i = 0; i < length * 2; i++) rdbLoadEncodedStringObject(file);
			return null;
		case REDIS_HASH_ZIPMAP:
		case REDIS_LIST_ZIPLIST:
		case REDIS_SET_INTSET:
		case REDIS_ZSET_ZIPLIST:
			rdbLoadStringObject(file);
			return null;
		default:
			throw new Exception("Unknown object type");
		}		
	}
	
	public void rdb_load(ByteBuffer file, Visitor visitor) throws Exception{
		String header = readStrFromFile(file, 5);
		if(!header.equals("REDIS")){
			throw new Exception("Wrong signature trying to learn DB from file");
		}
		String version = readStrFromFile(file, 4);
		int rdbVersion = Integer.parseInt(version);
		if(rdbVersion < 1 || 2 < rdbVersion){
			throw new Exception("Can't handle RDB format version " + rdbVersion);
		}
		int type = 0;
		do{
			type = rdbLoadType(file);
			if(type == REDIS_EXPIRETIME){
				rdbLoadTime(file);
				type =  rdbLoadType(file);				
			}
			if(type == REDIS_SELECTDB){
				rdb_load_length(file);
			}
			if(type != REDIS_EOF && type != REDIS_SELECTDB){
				String key = rdbLoadStringObject(file);
				String value = rdbLoadObject(type, file);
				visitor.callback(key, value);
			}
			
		}while(type != REDIS_EOF);
	}
	
	private String readStrFromFile(ByteBuffer file, int length){
		byte[] buf = new byte[length];
		for(int i = 0; i < length; i++){
			buf[i] = file.get();
		}
		return new String(buf);
	}
}

