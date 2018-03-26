
## Inner join between 2 Datasets Using MapReduce

### Objective: -
* To perform an inner join between 2 datasets.
* 2 datasets are instagram_4m.csv and instagram.csv
* Final output should contain PhotoId (key) present in both the files and will be of the format PhotoId (key), CreatedDt (from instagram.csv) and Likes (from Instagram.csv).
* Complete code can be found [here](Code/MapReduceJoin.java).

#### Input Format (instagram_4m.csv) : -
[UserId, PhotoId, Filter, Likes]

![png](File1.png)


#### Input Format (instagram_time.csv) : -
[PhotoId, CreatedDtTime]

![png](file2.png)

#### First Mapper Function
* Read and parse file 1 using this mapper.
* 2nd column (index 1) is taken as key and 4th column (index 3) is taken as value.
* A label Table1 is attached with values so that it helps us to make necessery calculations in Reducer.

 
		public void map(Object key, Text value, Context context				//Map function to parse 1st file
                 ) throws IOException, InterruptedException {
		   String record = value.toString();
		   String[] parts= record.split(",");
		   context.write(new Text(parts[1]), new Text("Table1\t" + parts[3]));
		   }
	}

#### Second Mapper Function
* Read and parse file 2 (instagram_time.csv) using this mapper.
* 1st column (index 0) is taken as key and 2nd column (index 1) is taken as value.
* A label Table2 is attached with values so that it helps us to make necessery calculations in Reducer.

		public void map(Object key, Text value, Context context)			//Map function to parse 2nd file
				throws IOException, InterruptedException {
			   String record = value.toString();
			   String[] parts= record.split(",");
			   context.write(new Text(parts[0]), new Text("Table2\t" + parts[1]));   
			}
	}

#### Reducer Function
* In reducer function if the label is Table1, it denoted the record is coming from File 1 and thus we capture value from Likes column.
* Likewise if the label is Table2, we capture CreatedDt column value.
* Flag is used to check if the given Key is having value present in both the files. This check ensures inner join action between two datasets.
* Context.write is placed inside the for loop to facilitate for 1 to many join conditions as well.

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
                        flag+=1;						//Flags are used to ensure record is present 
                                                                             	in both the files
					likes= parts[1];
				}else if(parts[0].equals("Table2")){
					flag+=1;
					photoId = key.toString();
					createdDt=parts[1];
				}										
										//Context.write inside for loop to ensure
                                                                               both 1-1 and 1-many join cases are handled
				if(flag==2){
					String str = String.format("%s\t%s",createdDt,likes);
					context.write(new Text(photoId),new Text(str));		       
				}
			}

		}

#### Driver Function


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

### Output File: -
[PhotoId, CreatedDt, Likes]
![png](outputfile.png)
