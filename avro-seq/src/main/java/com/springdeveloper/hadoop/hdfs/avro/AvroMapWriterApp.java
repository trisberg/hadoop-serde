package com.springdeveloper.hadoop.hdfs.avro;

import com.springdeveloper.domain.TweetMap;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
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
public class AvroMapWriterApp {

	private static ObjectMapper mapper = new ObjectMapper();
	private static TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {};

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + AvroMapWriterApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(AvroMapWriterApp.class.getSimpleName() + " [hdfs input dir] [hdfs output path]");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path destFile = new Path(args[1]);
		OutputStream os = fs.create(destFile);
		write(new File(args[0]), os);
	}

	public static void write(File inputFile, OutputStream outputStream) throws IOException {

		Schema schema = ReflectData.get().getSchema(TweetMap.class);

		System.out.println("SCHEMA: " + schema);

		DataFileWriter<TweetMap> writer =
				new DataFileWriter<TweetMap>(
					new ReflectDatumWriter<TweetMap>())
					.setSyncInterval(1 << 20)
					.setCodec(CodecFactory.snappyCodec())
					.create(schema, outputStream);

		try {
			for (String line : FileUtils.readLines(inputFile)) {
				TweetMap tweet = parseTweet(line);
				System.out.println("Writing: " + tweet);
				writer.append(tweet);
			}
		} finally {
			writer.close();
		}

	}

	private static TweetMap parseTweet(String json) throws IOException {
		HashMap<String, Object> map = mapper.readValue(json, type);
		HashMap<String, String> t = new HashMap<String, String>();
		t.put("id", map.get("id").toString());
		t.put("createdAt", map.get("createdAt").toString());
		t.put("fromUser", (String) map.get("fromUser"));
		t.put("text", (String) map.get("text"));
		TweetMap tm = new TweetMap();
		tm.setMap(t);
		return tm;
	}
}
