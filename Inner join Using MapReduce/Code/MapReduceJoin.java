import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceJoin {
	public static class Table1Map
    extends Mapper<Object, Text, Text, Text>{

 
		public void map(Object key, Text value, Context context						//Map function to parse 1st file
                 ) throws IOException, InterruptedException {
		   String record = value.toString();
		   String[] parts= record.split(",");
		   context.write(new Text(parts[1]), new Text("Table1\t" + parts[3]));
		   }
	}


	public static class Table2Map
    extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context)					//Map function to parse 2nd file
				throws IOException, InterruptedException {
			   String record = value.toString();
			   String[] parts= record.split(",");
			   context.write(new Text(parts[0]), new Text("Table2\t" + parts[1]));   
			}
	}
	
	public static class JoinReducer
	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String photoId="";
			String likes="";
			String createdDt="";
			int flag=0;
			for (Text t: values){
				String parts[] = t.toString().split("\t"); 
				if(parts[0].equals("Table1")){
					flag+=1;														//Flags are used to ensure record is present in both the files
					likes= parts[1];
				}else if(parts[0].equals("Table2")){
					flag+=1;
					photoId = key.toString();
					createdDt=parts[1];
				}
				if(flag==2){
					String str = String.format("%s\t%s",createdDt,likes);
					context.write(new Text(photoId),new Text(str));					//Context.write inside for loop to ensure both 1-1 and 1-many join cases are handled
				}
			}

		}
}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Map Reduce Join");
		job.setJarByClass(MapReduceJoin.class);


		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Table1Map.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Table2Map.class);

		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
