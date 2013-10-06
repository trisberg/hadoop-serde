package com.springdeveloper.hadoop.hdfs.avro;

import com.springdeveloper.domain.gen.Tweet;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
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
public class AvroGenWriterApp {

	private static ObjectMapper mapper = new ObjectMapper();
	private static TypeReference<HashMap<String, Object>> type = new TypeReference<HashMap<String, Object>>() {};

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + AvroGenWriterApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(AvroGenWriterApp.class.getSimpleName() + " [hdfs input dir] [hdfs output path]");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path destFile = new Path(args[1]);
		OutputStream os = fs.create(destFile);
		write(new File(args[0]), os);
	}

	public static void write(File inputFile, OutputStream outputStream) throws IOException {

		DataFileWriter<Tweet> writer =
				new DataFileWriter<Tweet>(
					new SpecificDatumWriter<Tweet>())
					.setSyncInterval(1 << 20)
					.setCodec(CodecFactory.snappyCodec())
					.create(Tweet.SCHEMA$, outputStream);

		try {
			for (String line : FileUtils.readLines(inputFile)) {
				Tweet tweet = parseTweet(line);
				System.out.println("Writing: " + tweet);
				writer.append(tweet);
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
