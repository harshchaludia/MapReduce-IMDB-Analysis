package original;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class imdb {
    public static class TokenizerMapper1
    extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String str = value.toString();
            String[] result = str.toString().split(";");
            if (result.length == 5) {
                int n = result.length;
                String year = result[n - 2];
				String type = result[n - 4];
                int setone = 0;
                for (int y = 2001; y <= 2015; y += 5)
                    if (!year.equals("\\N") && type.equals("movie")) {
                        setone = 0;
                        int yub = y + 4;
                        if (Integer.parseInt(year) >= y && Integer.parseInt(year) <= yub) {
                            String genreList = result[n - 1];
                            String genreArray[] = genreList.split(",");
                            for (String genre: genreArray) {
                                if (!genre.equals("\\N")) {
                                    if (genre.equals("Comedy")) {
                                        setone++;
                                    }
                                    if (genre.equals("Romance")) {
                                        setone++;
                                    }


                                }
                                if (setone == 2) {
                                    String intermidiateKey = "Comedy,Romance" + " " + y + "-" + yub;
                                    word.set(intermidiateKey);
                                    context.write(word, one);
                                }
                            }
                        }
                    }
            }
        }
    }

    public static class TokenizerMapper2
    extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String str = value.toString();
            String[] result = str.toString().split(";");
            if (result.length == 5) {
                int n = result.length;
                String year = result[n - 2];
				String type = result[n - 4];
				
                int settwo = 0;
                for (int y = 2001; y <= 2015; y += 5)
                    if (!year.equals("\\N") && type.equals("movie")) {
                        settwo = 0;
                        int yub = y + 4;
                        if (Integer.parseInt(year) >= y && Integer.parseInt(year) <= yub) {
                            String genreList = result[n - 1];
                            String genreArray[] = genreList.split(",");
                            for (String genre: genreArray) {
                                if (!genre.equals("\\N")) {
                                    if (genre.equals("Action")) {
                                        settwo++;
                                    }
                                    if (genre.equals("Thriller")) {
                                        settwo++;
                                    }
                                }
                                if (settwo == 2) {
                                    String intermidiateKey = "Action,Thriller" + " " + y + "-" + yub;
                                    word.set(intermidiateKey);
                                    context.write(word, one);
                                }
                            }
                        }
                    }
            }
        }
    }




    public static class TokenizerMapper3
    extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String str = value.toString();
            String[] result = str.toString().split(";");
            if (result.length == 5) {
                int n = result.length;
                String year = result[n - 2];
				String type = result[n - 4];

                int advsci = 0;
                for (int y = 2001; y <= 2015; y += 5)
                    if (!year.equals("\\N") && type.equals("movie")) {
                        advsci = 0;
                        int yub = y + 4;
                        if (Integer.parseInt(year) >= y && Integer.parseInt(year) <= yub) {
                            String genreList = result[n - 1];
                            String genreArray[] = genreList.split(",");
                            for (String genre: genreArray) {
                                if (!genre.equals("\\N")) {
                                    if (genre.equals("Adventure")) {
                                        advsci++;
                                    }
                                    if (genre.equals("Sci-Fi")) {
                                        advsci++;
                                    }
                                }
                                if (advsci == 2) {
                                    String intermidiateKey = "Adventure,Sci-Fi" + " " + y + "-" + yub;
                                    word.set(intermidiateKey);
                                    context.write(word, one);
                                }
                            }
                        }
                    }
            }
        }
    }








    public static class IntSumReducer
    extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < IntWritable > values,
            Context context
        ) throws IOException,
        InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);

            String strArr[] = key.toString().split(" ");
            if (strArr.length > 1) {
                String year = strArr[1];
                String genre = strArr[0];

                String newKey = year + "," + genre + ",";
                key.set(newKey);
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Path one = new Path(args[1]);
        Path two = new Path(args[2]);
        Path three = new Path(args[3]);

        //Path three = new Path(args[2]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(imdb.class);
        job.setMapperClass(TokenizerMapper1.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        if (fs.exists(one))
            fs.delete(one, true);
        FileOutputFormat.setOutputPath(job, one);
        job.waitForCompletion(true);
        //another mapper
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(imdb.class);
        job1.setMapperClass(TokenizerMapper2.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        if (fs.exists(two))
            fs.delete(two, true);
        FileOutputFormat.setOutputPath(job1, two);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "word count");
        job2.setJarByClass(imdb.class);
        job2.setMapperClass(TokenizerMapper3.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        if (fs.exists(three))
            fs.delete(three, true);
        FileOutputFormat.setOutputPath(job2, three);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}