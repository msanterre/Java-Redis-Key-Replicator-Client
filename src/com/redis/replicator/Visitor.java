package com.redis.replicator;

public interface Visitor {
	public void callback(String key, String value);
}
