package com.springdeveloper.hadoop.hdfs.avro;

import com.springdeveloper.domain.TweetJson;
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

/**
 */
public class AvroJsonWriterApp {

	private static ObjectMapper mapper = new ObjectMapper();
	private static TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {};

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + AvroJsonWriterApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(AvroJsonWriterApp.class.getSimpleName() + " [hdfs input dir] [hdfs output path]");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path destFile = new Path(args[1]);
		OutputStream os = fs.create(destFile);
		write(new File(args[0]), os);
	}

	public static void write(File inputFile, OutputStream outputStream) throws IOException {

		Schema schema = ReflectData.get().getSchema(TweetJson.class);

		System.out.println("SCHEMA: " + schema);

		DataFileWriter<TweetJson> writer =
				new DataFileWriter<TweetJson>(
					new ReflectDatumWriter<TweetJson>())
					.setSyncInterval(1 << 20)
					.setCodec(CodecFactory.snappyCodec())
					.create(schema, outputStream);

		try {
			for (String line : FileUtils.readLines(inputFile)) {
				TweetJson tweet = parseTweet(line);
				System.out.println("Writing: " + tweet);
				writer.append(tweet);
			}
		} finally {
			writer.close();
		}

	}

	private static TweetJson parseTweet(String json) throws IOException {
		HashMap<String, Object> map = mapper.readValue(json, type);
		HashMap<String, Object> t = new HashMap<String, Object>();
		t.put("id", map.get("id"));
		t.put("createdAt", map.get("createdAt"));
		t.put("fromUser", map.get("fromUser"));
		t.put("text", map.get("text"));
		TweetJson tm = new TweetJson();
		tm.setJson(mapper.writeValueAsString(t));
		return tm;
	}
}
