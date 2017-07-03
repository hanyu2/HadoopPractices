package test;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static class InvertedIndexMap extends Mapper<Object, Text, Text, Text> {

		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit split;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			split = (FileSplit) context.getInputSplit();
			StringTokenizer stk = new StringTokenizer(value.toString());
			while (stk.hasMoreElements()) {
				keyInfo.set(stk.nextToken() + ":" + split.getPath().toString());
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
		}
	}

	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

		Text info = new Text();

		public void reduce(Text key, Iterable<Text> values, Context contex) throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				sum += Integer.parseInt(value.toString());
			}

			int splitIndex = key.toString().indexOf(":");
			info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
			key.set(key.toString().substring(0, splitIndex));
			contex.write(key, info);
		}
	}

	public static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String fileList = new String();
			for (Text value : values) {
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			context.write(key, result);
		}
		
		
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "InvertedIndex");

		job.setJarByClass(InvertedIndex.class);

		job.setMapperClass(InvertedIndexMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(InvertedIndexCombiner.class);

		job.setReducerClass(InvertedIndexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}