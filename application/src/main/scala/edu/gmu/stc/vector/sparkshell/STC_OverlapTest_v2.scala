package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_OverlapTest.{logError, logInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}

/**
  * Created by Fei Hu on 1/31/18.
  */
object STC_OverlapTest_v2 extends Logging{
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      logError("Please input three arguments: " +
        "\n \t 1)configFilePath: this file path for the configuration file path" +
        "\n \t 2) numPartition: the number of partitions" +
        "\n \t 3) gridType: the type of the partition, e.g. EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE" +
        "\n \t 4) output file path: the file path for geojson output")

      return
    }

    val sparkConf = new SparkConf().setAppName("%s_%s_%s".format(STC_OverlapTest.toString, args(1), args(2)))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

    if (System.getProperty("os.name").equals("Mac OS X")) {
      sparkConf.setMaster("local[6]")
    }

    val sc = new SparkContext(sparkConf)

    val configFilePath = args(0)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val tableNames = hConf.get(ConfigParameter.SHAPEFILE_INDEX_TABLES).split(",").map(s => s.toLowerCase().trim)

    val partitionNum = args(1).toInt  //24
    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val gridType = GridType.getGridType(args(2)) //EQUALGRID, HILBERT, RTREE, VORONOI, QUADTREE, KDBTREE

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDDAndPartitioner(sc, table1, gridType, partitionNum, minX, minY, maxX, maxY)
    val geometryRDD1 = new GeometryRDD
    geometryRDD1.initialize(shapeFileMetaRDD1, hasAttribute = false)
    geometryRDD1.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD1.indexPartition(IndexType.RTREE)
    geometryRDD1.cache()
    logInfo("******geometryRDD1****************" + geometryRDD1.getGeometryRDD.count())

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = tableNames(1)
    shapeFileMetaRDD2.initializeShapeFileMetaRDDWithoutPartition(sc, table2,
      partitionNum, minX, minY, maxX, maxY)

    val geometryRDD2 = new GeometryRDD
    geometryRDD2.initialize(shapeFileMetaRDD2, hasAttribute = false)
    geometryRDD2.partition(shapeFileMetaRDD1.getPartitioner)
    geometryRDD2.cache()
    logInfo("******geometryRDD2****************" + geometryRDD2.getGeometryRDD.count())

    logInfo(geometryRDD1.getGeometryRDD.partitions.length
      + "**********************"
      + geometryRDD2.getGeometryRDD.partitions.length)

    val geometryRDD = geometryRDD1.intersect(geometryRDD2)
    //geometryRDD.saveAsGeoJSON(args(3))
    val filePath = args(3)
    if (filePath.endsWith("shp")) {
      geometryRDD.saveAsShapefile(filePath)
    } else {
      geometryRDD.saveAsGeoJSON(filePath)
    }

    //logInfo("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))
  }

}