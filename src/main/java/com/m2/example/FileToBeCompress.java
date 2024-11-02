//package com.m2.example;
//
//import org.apache.avro.Schema;
//import org.apache.avro.file.DataFileReader;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumReader;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
//import parquet.example.data.Group;
//import org.apache.parquet.hadoop.ParquetFileReader;
//import parquet.hadoop.ParquetInputFormat;
//import parquet.hadoop.ParquetOutputFormat;
//import parquet.hadoop.example.ExampleInputFormat;
//import parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.hadoop.util.HadoopInputFile;
//import org.apache.parquet.schema.MessageType;
//
//import java.io.File;
//import java.io.IOException;
//
//public class FileToBeCompress {//extends	Configured implements Tool
//
//    static Schema FILE_SCHEMA ;
//    /* public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//         @Override
//         public void reduce(Text key, Iterable<IntWritable> values, Context context)
//                 throws IOException, InterruptedException {
//             int sum = 0;
//             for (IntWritable i : values) {
//                 sum += i.get();
//             }
//
//             context.write(key, new IntWritable(sum));
//         }
//     }*/
///*
//* public static void main(String[] args)  throws Exception{
//		 int exitFlag = ToolRunner.run(new FileCompress(), args);
//		 System.exit(exitFlag);
//
//	}
//	*/
//    public static void main(String[] args) throws Exception {
//
//        if (args.length != 2) {
//            System.err.println("Usage: FileCompress <InPath> <OutPath>");
//            System.exit(2);
//        }
////	@Override
////	public int run(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        // conf.set(ParquetOutputFormat.COMPRESSION, "GZIP");
//        Job job = Job.getInstance(conf, "Compression");
//
//        job.setJarByClass(FileCompress.class);
//        job.setMapperClass(Map.class);
//        //job.setReducerClass(Reduce.class);
//        job.setNumReduceTasks(0);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//           job.setInputFormatClass(ExampleInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class); //AvroParquetOutputFormat TextOutputFormat
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//         FileOutputFormat.setCompressOutput(job, true);
//            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//
//      //  ParquetOutputFormat.setWriteSupportClass(job, ParquetWriteSupport.class);
//       // ParquetOutputFormat.setOutputPath(job, new Path(args[1]));
//    //    ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
//        //  SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//
//     public static MessageType getSchema(String filePath) throws IOException {
//         ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
//         MessageType schema = reader.getFooter().getFileMetaData().getSchema();
//        //System.out.println(dataFileReader.getMetaString("avro.codec"));
//  System.out.println("schme=="+schema);
//        return schema;
//    }
//
//    public static class Map extends Mapper<LongWritable, Group, NullWritable, Group> {
//
//        @Override
//        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
//
//             context.write(null, value);
//        }
//    }
//}
