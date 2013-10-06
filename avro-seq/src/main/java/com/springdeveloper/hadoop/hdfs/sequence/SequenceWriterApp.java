package com.springdeveloper.hadoop.hdfs.sequence;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.File;
import java.io.IOException;

/**
 */
public class SequenceWriterApp {

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + SequenceWriterApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(SequenceWriterApp.class.getSimpleName() + " [hdfs input dir] [hdfs output path]");
			System.exit(1);
		}
		write(new File(args[0]), new Path(args[1]));
	}

	public static void write(File inputFile, Path outputPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Writer writer =
				SequenceFile.createWriter(fs, conf, outputPath, Text.class, Text.class,
						SequenceFile.CompressionType.BLOCK,
						new DefaultCodec());
		try {
			Text key = new Text();
			int i = 0;
			for (String line : FileUtils.readLines(inputFile)) {
				System.out.println("Writing [" + i + "] " + line);
				Text value = new Text(line);
				key.set("Line " + i++);
				writer.append(key, value);
			}
		} finally {
			writer.close();
		}

	}
}
