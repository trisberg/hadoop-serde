package com.springdeveloper.domain;

import java.util.HashMap;

/**
 */
public class TweetMap {

	HashMap<String, String> map;

	public HashMap<String, String> getMap() {
		return map;
	}

	public void setMap(HashMap<String, String> map) {
		this.map = map;
	}

	@Override
	public String toString() {
		return "Tweet: [" + map.get("id") + "] " + map.get("text");
	}
}
