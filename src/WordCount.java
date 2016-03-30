
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
	
	private enum Compteurs{ UN;
		
	}
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Counter compteur;
		private HashMap<String, Integer> tampon;
		// private final static IntWritable ONE = new IntWritable(1);
		
		@Override
		protected void setup(Context context){
			tampon = new HashMap<String, Integer>();
			compteur = context.getCounter(Compteurs.UN);
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			 
			for(String key: tampon.keySet()){
				context.write(new Text(key), new IntWritable(tampon.get(key)));
			}
			tampon.clear();
			
			//context.write(text, ONE);

		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().replaceAll("[\\s]", "").equals("")){
				compteur.increment(1);
			}
			for (String word : value.toString().replaceAll("[^0-9a-zA-Z ]", "").toLowerCase().split("\\s+")) {
				if (!word.isEmpty()) {
					//context.write(text, ONE);
					if(tampon.containsKey(word)){
						tampon.put(word,tampon.get(word)+1);
					}else{
						tampon.put(word,1); 	
					}
					
				}
			}
		}

	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
		private LongWritable longwrit = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			long sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			longwrit.set(sum);
			context.write(key, longwrit);
		}

	}
	
	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable intwrit  = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			intwrit.set(sum);
			context.write(key, intwrit);
		}

	}
	

	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}