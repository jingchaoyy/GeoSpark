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
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery


object GeoSpark_JoinTest_V1 {

  // Initiate SparkContext
  val conf = new SparkConf()
  conf.setAppName("GeoSpark-Join") // Change this to a proper name
  conf.setMaster("local[*]") // Delete this if run in cluster mode
  // Enable GeoSpark custom Kryo serializer
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  val sc = new SparkContext(conf)


  // Create two generic SpatialRDD from shp
  val shapefileInputLocation1="data/Washington_DC/Soil_Type_by_Slope"
  val shapefileInputLocation2="data/Washington_DC/Impervious_Surface_2015"

  val sparkSession: SparkSession = SparkSession.builder()
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .master("local[*]")
    .appName("GeoSpark-Join")
    .getOrCreate()

  var spatialRDD1 = new SpatialRDD[Geometry]
  spatialRDD1.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation1)

  var spatialRDD2 = new SpatialRDD[Geometry]
  spatialRDD2.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation2)

  // Transform the Coordinate Reference System
//  var objectRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
  val sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
  val targetCrsCode = "epsg:3857" // The most common meter-based CRS
//  objectRDD.CRSTransform(sourceCrsCode, targetCrsCode)
  spatialRDD1.CRSTransform(sourceCrsCode, targetCrsCode)
  spatialRDD2.CRSTransform(sourceCrsCode, targetCrsCode)

  // Read other attributes in an SpatialRDD
  val rddWithOtherAttributes1 = spatialRDD1.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])
  val rddWithOtherAttributes2 = spatialRDD2.rawSpatialRDD.rdd.map[String](f=>f.getUserData.asInstanceOf[String])

  val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
  val usingIndex = false

  spatialRDD1.analyze()

  spatialRDD1.spatialPartitioning(GridType.KDBTREE)
  spatialRDD2.spatialPartitioning(spatialRDD1.getPartitioner)

  val result = JoinQuery.SpatialJoinQuery(spatialRDD1, spatialRDD2, usingIndex, considerBoundaryIntersection)

}

