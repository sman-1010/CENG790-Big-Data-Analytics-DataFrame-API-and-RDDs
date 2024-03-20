package edu.metu.ceng790.hw1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Part1 {
  def main(args: Array[String]): Unit = {
    val  spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))

      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("flickrSample.txt")

      // YOUR CODE HERE
    println(originalFlickrMeta.count())

    // Step 1: Select specific fields using Spark SQL
    originalFlickrMeta.createOrReplaceTempView("flickrData")

    val selectedFieldsDF = spark.sql("""
  SELECT
    photo_id,
    longitude,
    latitude,
    license
  FROM flickrData
""")

    // Step 2: Filter for interesting pictures
    val interestingPicturesDF = selectedFieldsDF
      .filter("license IS NOT NULL AND longitude != -1.0 AND latitude != -1.0")

    // Step 3: Display the execution plan
    interestingPicturesDF.explain()

    // Display the data of the interesting pictures
    interestingPicturesDF.show()

    // Read the second file FlickrLicense,txt
    val licenseSchema = StructType(Array(
      StructField("Name", StringType, true),
      StructField("Attribution", IntegerType, true),
      StructField("Noncommercial", IntegerType, true),
      StructField("NonDerivative", IntegerType, true),
      StructField("ShareAlike", IntegerType, true),
      StructField("PublicDomainDedication", IntegerType, true),
      StructField("PublicDomainWork", IntegerType, true)
    ))

    val licensesDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(licenseSchema)
      .csv("FlickrLicense.txt")

    // Perform Join
    val nonDerivativePicturesDF = interestingPicturesDF
      .join(licensesDF, interestingPicturesDF("license") === licensesDF("Name"))
      .filter(licensesDF("NonDerivative") === 1)


    // Examine Execution Plan
    nonDerivativePicturesDF.explain()

    // Display Results
    nonDerivativePicturesDF.show()

    //cache the result
    interestingPicturesDF.cache()
    interestingPicturesDF.count()

    val nonDerivativePicturesDF2 = interestingPicturesDF
      .join(licensesDF, interestingPicturesDF("license") === licensesDF("Name"))
      .filter(licensesDF("NonDerivative") === 1)

    nonDerivativePicturesDF2.explain()

    // Specify the path where you want to save the CSV file
    val outputPath = "/Users/m9/IdeaProjects/CENG790-Big Data Analytics-DataFrame API and RDDs/nonDerivativePictures.csv"
    // Save the DataFrame to a CSV file with a header
    nonDerivativePicturesDF.write
      .option("header", "true") // Include column names as the first row in the output
      .csv(outputPath)

  }
}
