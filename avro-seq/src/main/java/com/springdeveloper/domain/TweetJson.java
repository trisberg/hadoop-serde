package com.springdeveloper.domain;

/**
 */
public class TweetJson {

	String json;

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

	@Override
	public String toString() {
		return "Tweet: " + json;
	}
}
