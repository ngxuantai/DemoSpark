/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.geodataprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GeoDataProcessing {

        public static void main(String[] args) {
                // Khởi tạo SparkSession
                SparkSession spark = SparkSession.builder()
                                .master("local")
                                .appName("GeoDataProcessing")
                                .getOrCreate();

                try {
                        // Đọc dữ liệu từ tệp CSV
                        Dataset<Row> data = spark.read()
                                        .format("csv")
                                        .option("header", "true")
                                        .load("C://Users/nosc/Desktop/text.csv");

                        // Tính tổng số địa điểm
                        long totalPlaces = data.count();
                        System.out.println("Total places: " + totalPlaces);

                        // Tính điểm trung bình của kinh độ và vĩ độ
                        double avgLongitude = data.agg(org.apache.spark.sql.functions.avg("longitude")).first()
                                        .getDouble(0);
                        // chuyển avgLongitude từ kiểu dữ liệu Double về kiểu dữ liệu long
                        avgLongitude = (long) avgLongitude;
                        double avgLatitude = data.agg(org.apache.spark.sql.functions.avg("latitude")).first()
                                        .getDouble(0);

                        System.out.println("Average Longitude: " + avgLongitude);
                        System.out.println("Average Latitude: " + avgLatitude);

                        // Lưu kết quả vào tệp văn bản
                        Dataset<Row> results = spark.createDataFrame(
                                        java.util.Arrays.asList(
                                                        new org.apache.spark.sql.RowFactory().create("Total Places",
                                                                        totalPlaces),
                                                        new org.apache.spark.sql.RowFactory().create(
                                                                        "Average Longitude",
                                                                        avgLongitude),
                                                        new org.apache.spark.sql.RowFactory().create("Average Latitude",
                                                                        (double) avgLatitude)),
                                        new org.apache.spark.sql.types.StructType(
                                                        new org.apache.spark.sql.types.StructField[] {
                                                                        new org.apache.spark.sql.types.StructField(
                                                                                        "Metric",
                                                                                        org.apache.spark.sql.types.DataTypes.StringType,
                                                                                        false,
                                                                                        org.apache.spark.sql.types.Metadata
                                                                                                        .empty()),
                                                                        new org.apache.spark.sql.types.StructField(
                                                                                        "Value",
                                                                                        org.apache.spark.sql.types.DataTypes.LongType,
                                                                                        false,
                                                                                        org.apache.spark.sql.types.Metadata
                                                                                                        .empty())
                                                        }));

                        results.write()
                                        .format("csv")
                                        .mode(org.apache.spark.sql.SaveMode.Overwrite)
                                        // .option("header", "true")
                                        .save("C://Users/nosc/Desktop/results.csv");

                } catch (Exception e) {
                        e.printStackTrace();
                } finally {
                        spark.stop();
                }
        }
}
