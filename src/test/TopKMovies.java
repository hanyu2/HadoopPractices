package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKMovies {
	static int K = 0;

	public static class TopKMoviesMap extends Mapper<Object, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split(",");
			keyInfo.set(lines[0]);
			valueInfo.set(lines[2]);
			context.write(keyInfo, valueInfo);
		}
	}

	public static class TopKMoviesCombiner extends Reducer<Text, Text, Text, Text> {
		PriorityQueue<Item> pq = new PriorityQueue<Item>(3, new Comparator<Item>() {
			public int compare(Item item1, Item item2) {
				return item1.rating - item2.rating > 0 ? 1 : -1;
			}
		});

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int K = Integer.parseInt(context.getConfiguration().get("K"));
			double sum = 0;
			int count = 0;
			for (Text value : values) {
				sum += Double.parseDouble(value.toString());
				count++;
			}
			double rating = sum / count;
			if(pq.size() < K){
				pq.add(new Item(key.toString(), rating));
			}else{
				if(rating > pq.peek().rating){
					pq.poll();
					pq.add(new Item(key.toString(), rating));
				}
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<Item> list = new ArrayList<Item>();
			while(!pq.isEmpty()){
				list.add(pq.poll());
			}
			for(int i = list.size() - 1; i >= 0; i--){
				Item item = list.get(i);
				context.write(new Text(item.movieId), new Text(String.valueOf(item.rating)));
			}
		}
	}

	public static class TopKMoviesReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("K", args[0]);
		try {
			Job job = Job.getInstance(conf, "TopKRatedMovie");
			job.setJarByClass(TopKMovies.class);
			job.setMapperClass(TopKMoviesMap.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setCombinerClass(TopKMoviesCombiner.class);
			job.setReducerClass(TopKMoviesReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/ratings.csv"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/output/movies"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class Item {
	String movieId;
	Double rating;

	public Item(String movieId, Double rating) {
		this.movieId = movieId;
		this.rating = rating;
	}
}
