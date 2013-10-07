package com.springdeveloper.hadoop.hdfs.thrift;

import com.hadoop.compression.lzo.LzopCodec;

import com.springdeveloper.domain.gen.Tweet;

import com.twitter.elephantbird.mapreduce.io.ThriftBlockReader;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class ThriftGenReaderApp {

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + ThriftGenReaderApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(ThriftGenReaderApp.class.getSimpleName() + " [hdfs input path] [hdfs output file]");
			System.exit(1);
		}
		Path sourceFile = new Path(args[0]);
		read(sourceFile);
	}

	public static void read(Path sourceFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		InputStream is = fs.open(sourceFile);

		LzopCodec codec = new LzopCodec();
		codec.setConf(conf);
		InputStream inputStream = codec.createInputStream(fs.open(sourceFile));

		ThriftBlockReader<Tweet> reader =
				new ThriftBlockReader<Tweet>(
						inputStream, new TypeRef<Tweet>() {});

		try {
			 Tweet t = null;
			 while ((t = reader.readNext()) != null) {
				System.out.println(ToStringBuilder.reflectionToString(t, ToStringStyle.SIMPLE_STYLE));
			}
		} finally {
			reader.close();
			inputStream.close();
		}

	}
}
