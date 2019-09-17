
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5); //usually noGram range is [5,8]
		}

		// map method -- outputKey = phrase
		//						   outputValue = 1
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			line = line.trim().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");

			String[] words = line.split("\\s+"); //split by ' ', '\t'...ect

			if(words.length<2) { //only one word, 1-gram is useless
				return;
			}

			//I love big data, n=3(build 3-gram): two pointers
			//I love,love big, big data
			//I love big, love big data
			StringBuilder sb;
			for(int i = 0; i < words.length-1; i++) { //i: start index
				sb = new StringBuilder();
				sb.append(words[i]);
				for(int j=1; i+j<words.length && j<noGram; j++) { //j: phrase length
					sb.append(" ");
					sb.append(words[i+j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method -- inputKey = phrase(mapper's outputKey)
		//							 -- inputValue = <1,1,1...1>
		//							 -- outputKey = phrase
		//							 -- outputValue = sum_phrase
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values) {
				sum += value.get(); //do not use sum += 1, incase of the value is not a IntWritable class(could be String)
			}
			context.write(key, new IntWritable(sum));
				//Hadoop default outputType:  "love big data\t230" : "phrase\tFreq"
		}
	}

}
