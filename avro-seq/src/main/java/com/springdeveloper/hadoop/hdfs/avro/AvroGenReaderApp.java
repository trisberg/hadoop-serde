package com.springdeveloper.hadoop.hdfs.avro;

import com.springdeveloper.domain.gen.Tweet;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class AvroGenReaderApp {

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + AvroGenReaderApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(AvroGenReaderApp.class.getSimpleName() + " [hdfs input path] [hdfs output file]");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path sourceFile = new Path(args[0]);
		InputStream inputStream = fs.open(sourceFile);
		read(inputStream);
	}

	public static void read(InputStream inputStream) throws IOException {

		DataFileStream<Tweet> reader =
				new DataFileStream<Tweet>(inputStream,
						new SpecificDatumReader<Tweet>(Tweet.class));
		try {
			for (Tweet t : reader) {
				System.out.println(ToStringBuilder.reflectionToString(t, ToStringStyle.SIMPLE_STYLE));
			}
		} finally {
			inputStream.close();
			reader.close();
		}

	}
}
