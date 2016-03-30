
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question3 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String list[] = value.toString().split("\t");

			Country pays = Country.getCountryAt(Double.valueOf(list[11]), Double.valueOf(list[10]));

			String tags = java.net.URLDecoder.decode(list[8], "UTF-8");
			if (pays != null) {
				for (String tag : tags.split(",")) {
					if(!tag.equals("")){
						context.write(new Text(pays.toString() + "," + tag ), new IntWritable(1));
					}
				}
			}
		}
	}
	
	public static class MyMapper2 extends Mapper<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void map(Text pays, StringAndInt value, Context context) throws IOException, InterruptedException {

			context.write(pays, value);
					
		}
	}
		
	
	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text pays_tag, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int somme = 0;
			for (IntWritable value : values) {
				somme = somme + value.get();	
			}
			
			context.write(pays_tag, new IntWritable(somme));
		}

	}
	
	public static class MyCombiner2 extends Reducer<Text, StringAndInt, Text, StringAndInt> {

		@Override
		protected void reduce(Text pays, Iterable<StringAndInt> values, Context context) throws IOException,
				InterruptedException {
			
			int[] max = new int[context.getConfiguration().getInt("K",1)];
			for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
				max[0]=0;
			}
			StringAndInt[] tagsOrdonnes = new StringAndInt[context.getConfiguration().getInt("K",1)];
			for (StringAndInt value : values) {
				for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
					if(max[i]<value.nb){
						for(int j=4; j>=i;j--){
							max[j+1]=max[j];
							tagsOrdonnes[j+1]=new StringAndInt(new Text(tagsOrdonnes[j].tag.toString()),tagsOrdonnes[j].nb);
						}
						max[i]=value.nb;
						tagsOrdonnes[i]=new StringAndInt(new Text(value.tag.toString()),value.nb);
						break;
					}
				}					
			}
			for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
				if(max[i]==0){
					break;
				}
				context.write(pays, tagsOrdonnes[i]);
			}
			
		}

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, StringAndInt> {

		@Override
		protected void reduce(Text pays_tag, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			

			int somme = 0;
			for (IntWritable value : values) {
				somme = somme + value.get();	
			}
			
			context.write(new Text(pays_tag.toString().split(",")[0]), new StringAndInt(new Text(pays_tag.toString().split(",")[1]),somme));
			
		}
	}
	
	public static class MyReducer2 extends Reducer<Text, StringAndInt, Text, Text> {

		@Override
		protected void reduce(Text pays, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			

			
			int[] max = new int[context.getConfiguration().getInt("K",1)];
			for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
				max[0]=0;
			}
			
			StringAndInt[] tagsOrdonnes = new StringAndInt[context.getConfiguration().getInt("K",1)];
			for (StringAndInt value : values) {
				for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
					if(max[i]<value.nb){
						for(int j=4; j>=i;j--){
							max[j+1]=max[j];
							tagsOrdonnes[j+1]=new StringAndInt(new Text(tagsOrdonnes[j].tag.toString()),tagsOrdonnes[j].nb);
						}
						max[i]=value.nb;
						tagsOrdonnes[i]= new StringAndInt(new Text(value.tag.toString()),value.nb);
						break;
					}
				}					
			}
			String resultat = "";
			for (int i=0; i<context.getConfiguration().getInt("K",1);i++){
				if(max[i]==0){
					break;
				}
				resultat = resultat + " " + tagsOrdonnes[i].tag.toString() + " " + max[i];
			}
			context.write(pays,new Text(resultat));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 5);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question3");
		job.setJarByClass(Question3.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyCombiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringAndInt.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path("intermediate_output"));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if(job.waitForCompletion(true)){
		Job job2 = Job.getInstance(conf, "Question3");
		job2.setJarByClass(Question3.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(StringAndInt.class);
		job2.setCombinerClass(MyCombiner2.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);
		

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}