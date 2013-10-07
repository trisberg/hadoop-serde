package com.springdeveloper.hadoop.hdfs.thrift;

import com.hadoop.compression.lzo.LzopCodec;

import com.springdeveloper.domain.gen.Tweet;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class ThriftGenWriterApp {

	private static ObjectMapper mapper = new ObjectMapper();
	private static TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {};

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + com.springdeveloper.hadoop.hdfs.thrift.ThriftGenWriterApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(com.springdeveloper.hadoop.hdfs.thrift.ThriftGenWriterApp.class.getSimpleName() + " [hdfs input dir] [hdfs output path]");
			System.exit(1);
		}
		Path destFile = new Path(args[1]);
		write(new File(args[0]), destFile);
	}

	public static void write(File inputFile, Path destFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		OutputStream os = fs.create(destFile);

		LzopCodec codec = new LzopCodec();
		codec.setConf(conf);

		OutputStream lzopOs = codec.createOutputStream(os);

		ThriftBlockWriter<Tweet> writer = new ThriftBlockWriter<Tweet>(lzopOs, Tweet.class);

		try {
			for (String line : FileUtils.readLines(inputFile)) {
				Tweet tweet = parseTweet(line);
				System.out.println("Writing: " + tweet);
				writer.write(tweet);
			}
		} finally {
			writer.close();
		}

	}

	private static Tweet parseTweet(String json) throws IOException {
		Map<String, Object> map = mapper.readValue(json, type);
		Tweet t = new Tweet();
		t.setId((Long) map.get("id"));
		t.setCreatedAt((Long) map.get("createdAt"));
		t.setFromUser((String) map.get("fromUser"));
		t.setText((String) map.get("text"));
		return t;
	}
}
