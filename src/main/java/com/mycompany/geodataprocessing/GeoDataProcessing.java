
package com.mycompany.geodataprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GeoDataProcessing {

        public static void main(String[] args) {
                SparkSession spark = SparkSession.builder()
                                .master("spark://laptop-pvpfh1bd:7077")
                                .appName("GeoDataProcessing")
                                .getOrCreate();

                try {
                        Dataset<Row> data = spark.read()
                                        .format("csv")
                                        .option("header", "true")
                                        .load("hdfs://localhost:9000/NameStudent.csv");

                        Dataset<Row> avgScorePerStudent = data.withColumn("Điểm trung bình",
                                        (data.col("Toán").cast("double")
                                                        .plus(data.col("Văn").cast("double"))
                                                        .plus(data.col("Lý").cast("double"))
                                                        .plus(data.col("Hóa").cast("double"))
                                                        .plus(data.col("Sinh").cast("double")))
                                                        .divide(5));

                        Dataset<Row> avgScorePerAge = avgScorePerStudent.groupBy("Tuổi")
                                        .avg("Điểm trung bình")
                                        .withColumnRenamed("avg(Điểm trung bình)", "Điểm trung bình trên theo tuổi");

                        // Lưu kết quả vào tệp văn bản
                        avgScorePerStudent.coalesce(1)
                                        .write()
                                        .format("csv")
                                        .mode("overwrite")
                                        .option("header", "true")
                                        .save("hdfs://localhost:9000/AverageScorePerStudent.csv");

                        avgScorePerAge.coalesce(1).write()
                                        .format("csv")
                                        .mode("overwrite")
                                        .option("header", "true")
                                        .save("hdfs://localhost:9000/AverageScorePerAge.csv");

                } catch (Exception e) {
                        e.printStackTrace();
                } finally {
                        spark.stop();
                }
        }
}
