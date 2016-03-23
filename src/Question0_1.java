
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question0_1 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String list[] = value.toString().split("\t");

			Country pays = Country.getCountryAt(Double.valueOf(list[11]), Double.valueOf(list[10]));

			String tags = java.net.URLDecoder.decode(list[8], "UTF-8");
			if (pays != null) {
				for (String tag : tags.split(",")) {
					context.write(new Text(pays.toString()), new StringAndInt(new Text(tag),1));
				}
			}
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {

		@Override
		protected void reduce(Text pays, Iterable<StringAndInt> SaIs, Context context) throws IOException,
				InterruptedException {
			HashMap<Text, Integer> tagNb = new HashMap<Text, Integer>();
			
			for (StringAndInt SaI : SaIs) {
				if (tagNb.containsKey(SaI.tag)) {
					tagNb.put(SaI.tag, tagNb.get(SaI.tag) + SaI.nb);
				} else {
					tagNb.put(new Text(SaI.tag), SaI.nb);
				}

			}
			
			
			for (Text tag : tagNb.keySet()) {
				context.write(pays, new StringAndInt(tag,tagNb.get(tag)));
			}
		}

	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {

		@Override
		protected void reduce(Text pays, Iterable<StringAndInt> SaIs, Context context)
				throws IOException, InterruptedException {
			
			HashMap<Text, Integer> tagNb = new HashMap<Text, Integer>();

			for (StringAndInt SaI : SaIs) {

				if (tagNb.containsKey(SaI.tag)) {
					tagNb.put(SaI.tag, tagNb.get(SaI.tag) + SaI.nb);
				} else {
					tagNb.put(new Text(SaI.tag), SaI.nb);
				}

			}				

			PriorityQueue<StringAndInt> pq = new PriorityQueue<StringAndInt>(100);
			
			for (Text tag : tagNb.keySet()){
				pq.add(new StringAndInt(tag, tagNb.get(tag)));
			}
			
			StringAndInt SaI;
			String tags = "";
			for (int i = 0; i < context.getConfiguration().getInt("K",1); i++) {
				SaI = pq.poll();
				if (SaI != null) {
					tags = tags + " " + SaI.tag.toString() + " " + SaI.nb;
				}
			}
			context.write(pays, new Text(tags));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 10);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question0_1");
		job.setJarByClass(Question0_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);
		job.setCombinerClass(MyCombiner.class);

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