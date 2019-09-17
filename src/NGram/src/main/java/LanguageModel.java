
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20); //filter out and skip the phrase freq < 20
		}


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//input: love big data\t230
			//love big | data=t230
			//outputKey = "input_phrase"
			//outputValue = "following_word=count"
			String line = value.toString().trim();

			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}

			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			if(count < threashold) {
				return;
			}

			//love big --> data=230
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length-1; i++) { //word[0] + .. + word[n-2]
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1]; //the last word (word to predicted)

			if(!((outputKey == null) || (outputKey.length() <1))) {
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
				// output: "love big", "data=230"
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n; //topK
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//inputKey = "input_phrase"
			//inputValue = <following_word=count1, fw2=count2... >
			//get topK -> write to database

			//value: <data = 230, apple = 230, boy = 50 ... >
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for(Text val: values) {
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}
			//<230, <apple, data>> <50, <boy...>>
			Iterator<Integer> iter = tm.keySet().iterator();
			for(int j=0; iter.hasNext() && j<n;) { //j:index in List（value）to chose topK prediction_word of this current key phrase
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for(String curWord: words) {
					//DBOutputWritable's input format is ResultSet class,  readFields(ResultSet arg0) 
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount),NullWritable.get());
					j++; //must at here, cannot in the outer for loop
				}
			}
		}
	}
}
