
/* Name - Yugandhara Kulkarni
 * Email - ykulkarn@uncc.edu
 * This program performs sorting on the results of search operations on a set of input files.*/

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);
	

	public static void main(String[] args) throws Exception {

		//to run toolrunner for the class.
		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		//to create job
		Job job = Job.getInstance(getConf(), " rank ");

		//to set jar
		job.setJarByClass(this.getClass());

		//to add path for input file
		FileInputFormat.addInputPaths(job, args[0]);

		//to add path for output file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//to set mapper
		job.setJarByClass(this.getClass());

		//to set reducer
		job.setMapperClass(Map.class);

		//to set data type of key for mapper
		job.setReducerClass(Reduce.class);

		//to set data type of output key
		job.setOutputKeyClass(DoubleWritable.class);

		//to set data type of output value
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion( true)  ? 0 : 1;

	}

	public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		//Map function which will generate intermediate key value pairs
		public void map(LongWritable offset, Text lineText, Context context)throws IOException, InterruptedException {

			String inputLine = lineText.toString();
			
			//to get the file names and term frequencies from input data
			String[] fileNameFrequencyArray = inputLine.toString().split("\t");
			Text currentWord = new Text(fileNameFrequencyArray[0]);
			
			Double TFIDFValue = (Double.parseDouble(fileNameFrequencyArray[1])*(-1));
			
			//frequency term as a key to get the key sorted
			context.write(new DoubleWritable(TFIDFValue), currentWord);
			

		}
	}

	public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		
		//Reducer that takes intermediate key value pairs from map as an input to display search results in descending order.
		@Override
		public void reduce(DoubleWritable term, Iterable<Text> counts, Context context)throws IOException, InterruptedException {			
			for (Text t : counts) {
				context.write(t, new DoubleWritable(term.get()*(-1)));
			}
			
		}

	}
}