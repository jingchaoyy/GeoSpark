/**
  * Created by Jingchao Yang on 5/2/18.
  */

package edu.gmu.stc.vector.sparkshell

import com.vividsolutions.jts.geom._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.spatialRDD.{CircleRDD, SpatialRDD}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery


object GeoSpark_JoinTest_V1 {

  def main(args: Array[String]): Unit = {

    // Initiate SparkContext
    val conf = new SparkConf()
    conf.setAppName("GeoSpark-Join") // Change this to a proper name
    conf.setMaster("local[*]") // Delete this if run in cluster mode
    // Enable GeoSpark custom Kryo serializer
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    conf.set("spark.kryoserializer.buffer.max.mb", "512")  //Avoiding memory overflow
//    val sc = new SparkContext(conf)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      conf.setMaster("local[6]")
    }

    val sc = new SparkContext(conf)

    // Create two generic SpatialRDD from shp
    val shapefileInputLocation1 = "data/Washington_DC/Soil_Type_by_Slope"
    val shapefileInputLocation2 = "data/Washington_DC/Impervious_Surface_2015"

    val sparkSession: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]")
      .appName("GeoSpark-Join")
      .getOrCreate()

    var spatialRDD1 = new SpatialRDD[Geometry]
    spatialRDD1.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation1)

    print("********* ", spatialRDD1.countWithoutDuplicates())
    var spatialRDD2 = new SpatialRDD[Geometry]
    spatialRDD2.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation2)
    print("********* ", spatialRDD2.countWithoutDuplicates())
    // Transform the Coordinate Reference System
    //  var objectRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
    val sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
    val targetCrsCode = "epsg:3857" // The most common meter-based CRS
    //  objectRDD.CRSTransform(sourceCrsCode, targetCrsCode)
    spatialRDD1.CRSTransform(sourceCrsCode, targetCrsCode)
    spatialRDD2.CRSTransform(sourceCrsCode, targetCrsCode)

    // Read other attributes in an SpatialRDD
    val rddWithOtherAttributes1 = spatialRDD1.rawSpatialRDD.rdd.map[String](f => f.getUserData.asInstanceOf[String])
    val rddWithOtherAttributes2 = spatialRDD2.rawSpatialRDD.rdd.map[String](f => f.getUserData.asInstanceOf[String])

    val considerBoundaryIntersection = true // Only return gemeotries fully covered by each query window in queryWindowRDD if set to false
//    val usingIndex = false

    spatialRDD1.analyze()

    spatialRDD1.spatialPartitioning(GridType.KDBTREE, 30)
    print("############", spatialRDD1.countWithoutDuplicates())
    spatialRDD2.spatialPartitioning(spatialRDD1.getPartitioner)
    print("############", spatialRDD2.countWithoutDuplicates())

    // Use spatial indexes
    // The index should be built on either one of two SpatialRDDs.
    // In general, you should build it on the larger SpatialRDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true
    spatialRDD2.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    print("################# Index build success")

    val result = JoinQuery.SpatialJoinQuery(
      spatialRDD1, spatialRDD2,
      usingIndex,
      considerBoundaryIntersection)


    print("******************** ", result.count())
    sparkSession.stop()
  }

}

