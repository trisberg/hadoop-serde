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
public class SequenceReaderApp {

	public static void main(String[] args) throws IOException {
		System.out.println("Running " + SequenceReaderApp.class.getSimpleName() + " ... ");
		if (args.length < 2) {
			System.err.println(SequenceReaderApp.class.getSimpleName() + " [hdfs input path] [hdfs output file]");
			System.exit(1);
		}
		read(new Path(args[0]), new File(args[1]));
	}

	public static void read(Path inputPath, File outputFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Reader reader =
				new SequenceFile.Reader(fs, inputPath, conf);
		try {
			System.out.println(
					"Is block compressed = " + reader.isBlockCompressed());

			Text key = new Text();
			Text value = new Text();

			while (reader.next(key, value)) {
				System.out.println(key + "," + value);
			}
		} finally {
			reader.close();
		}

	}
}
