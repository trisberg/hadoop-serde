package com.springdeveloper.hadoop.hdfs.avro;

import com.springdeveloper.domain.Tweet;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class AvroReflectReaderApp {

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + AvroReflectReaderApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(AvroReflectReaderApp.class.getSimpleName() + " [hdfs input path] [hdfs output file]");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path sourceFile = new Path(args[0]);
		InputStream inputStream = fs.open(sourceFile);
		read(inputStream);
	}

	public static void read(InputStream inputStream) throws IOException {

		Schema schema = ReflectData.get().getSchema(Tweet.class);

		DataFileStream<Tweet> reader =
				new DataFileStream<Tweet>(inputStream,
						new ReflectDatumReader<Tweet>(schema));
		try {
			for (Tweet t : reader) {
				System.out.println(t);
			}
		} finally {
			inputStream.close();
			reader.close();
		}

	}
}
