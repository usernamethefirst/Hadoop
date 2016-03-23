
import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question0_0 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String list[] = value.toString().split("\t");

			Country pays = Country.getCountryAt(Double.valueOf(list[11]), Double.valueOf(list[10]));

			String tags = java.net.URLDecoder.decode(list[8], "UTF-8");
			if (pays != null) {
				for (String tag : tags.split(",")) {
					context.write(new Text(pays.toString()), new Text(tag));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> tagNb = new HashMap<String, Integer>();

			for (Text value : values) {

				if (tagNb.containsKey(value.toString())) {
					tagNb.put(value.toString(), tagNb.get(value.toString()) + 1);
				} else {
					tagNb.put(value.toString(), 1);
				}

			}

			PriorityQueue<StringAndInt> pq = new PriorityQueue<StringAndInt>(100);

			for (String tag : tagNb.keySet()) {
				pq.add(new StringAndInt(tag, tagNb.get(tag)));
			}
			StringAndInt sai;
			String tags = "";
			for (int i = 0; i < context.getConfiguration().getInt("K",1); i++) {
				sai = pq.poll();
				if (sai != null) {
					tags = tags + " " + sai.tag;
				}
			}
			context.write(key, new Text(tags));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 5);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question0_0.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}