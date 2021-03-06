package edu.gmu.stc.vector.shapefile.reader;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.SystemClock;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.FieldDescriptor;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by Jingchao Yang on 5/22/18.
 */
public class GeometryReaderUtil_Header {
  /** suffix of attribute file */
  public final static String DBF_SUFFIX = ".dbf";

  /** suffix of shape record file */
  public final static String SHP_SUFFIX = ".shp";

  /** suffix of index file */
  public final static String SHX_SUFFIX = ".shx";

  public final static List<String> headerName = new ArrayList<>();

  public static Geometry readGeometryWithAttributes(FSDataInputStream shpDataInputStream,
                                                    FSDataInputStream dbfDataInputStream,
                                                    ShapeFileMeta shapeFileMeta,
                                                    DbfParseUtil dbfParseUtil,
                                                    GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(),
                            content,
                            0,
                            shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());


    PrimitiveShape value = new PrimitiveShape(shpRecord);

    byte[] primitiveBytes = new byte[shapeFileMeta.getDbf_length()];
    dbfDataInputStream.read(shapeFileMeta.getDbf_offset(),
                            primitiveBytes,
                            0,
                            shapeFileMeta.getDbf_length());
    String attributes = dbfParseUtil.primitiveToAttributes(ByteBuffer.wrap(primitiveBytes));
    value.setAttributes(attributes);

    return value.getShape(geometryFactory);
  }

  public static Geometry readGeometry(FSDataInputStream shpDataInputStream,
                                      ShapeFileMeta shapeFileMeta,
                                      GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(), content,
                            0, shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());
    PrimitiveShape value = new PrimitiveShape(shpRecord);
    return value.getShape(geometryFactory);
  }

  public static List<Geometry> readGeometriesWithAttributes(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil_Header.SHP_SUFFIX);
    Path dbfFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil_Header.DBF_SUFFIX);
    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    FSDataInputStream dbfInputStream = fs.open(dbfFilePath);
    DbfParseUtil dbfParseUtil = new DbfParseUtil();
    dbfParseUtil.parseFileHead(dbfInputStream);
    GeometryFactory geometryFactory = new GeometryFactory();
//    dbfParseUtil.getFieldDescriptors();
    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil_Header.readGeometryWithAttributes(shpInputStream, dbfInputStream,
                                                                   shapeFileMeta, dbfParseUtil,
                                                                   geometryFactory));
    }

    return geometries;
  }

  public static List<String> getDbfHeader(String dbfFilePath) throws IOException {
    Path dbfFilePathHeader = new Path(dbfFilePath);
    FileSystem fsHeader = dbfFilePathHeader.getFileSystem(new Configuration());
    FSDataInputStream dbfInputStreamHeader = fsHeader.open(dbfFilePathHeader);
    DbfParseUtil dbfParseUtilHeader = new DbfParseUtil();
    dbfParseUtilHeader.parseFileHead(dbfInputStreamHeader);
    List<FieldDescriptor> fieldDescriptors = dbfParseUtilHeader.getFieldDescriptors();

    List<String> headers = new ArrayList<>();
    for (int i = 0; i < fieldDescriptors.size(); i++){
      headers.add(fieldDescriptors.get(i).getFiledName());
    }

    return headers;

  }

  public static List<Geometry> readGeometries(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil_Header.SHP_SUFFIX);

    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    GeometryFactory geometryFactory = new GeometryFactory();

    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil_Header.readGeometry(shpInputStream, shapeFileMeta, geometryFactory));
    }

    return geometries;
  }

  public static String truncate(String value, int length) {
    // Ensure String length is longer than requested size.

    if (value.length() > length) {
      String tru = value.substring(0, length);
      if (headerName.contains(tru)) {
        headerName.add(tru.substring(0, length-2) + "_1");
        return tru.substring(0, length-2) + "_1";
      }
      else {
        headerName.add(tru);
        return tru;
      }
    } else {
      if (headerName.contains(value)){
        if (value.length() > length-2){
          headerName.add(value.substring(0, length-2) + "_1");
          return value.substring(0, length-2) + "_1";
        }
        else{
          headerName.add(value + "_1");
          return value + "_1";
        }
      }
      else{
        headerName.add(value);
        return value;
      }
    }
  }

//  public static String truncate(String value, int length) {
//    // Ensure String length is longer than requested size.
//
//    if (value.length() > length)
//      return value.substring(0, length);
//    else
//        return value;
//  }

  public static void saveAsShapefile(String filepath, List<Geometry> geometries, String crs, String header)
      throws IOException, FactoryException {
    System.out.println(
            "*****************start writing shapefile ******************");
    Long startTime = System.currentTimeMillis();
    File file = new File(filepath);
    Map<String, Serializable> params = new HashMap<String, Serializable>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
    ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

    SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();

    CoordinateReferenceSystem crsType = CRS.decode(crs);

    tb.setCRS(crsType);

    tb.setName("shapefile");
    tb.add("the_geom", Polygon.class);
    tb.add("outPolyID", Long.class);
    tb.add("minLat", Double.class);
    tb.add("minLon", Double.class);
    tb.add("maxLat", Double.class);
    tb.add("maxLon", Double.class);
//    tb.add(header, String.class);

    List<String> headerList = Arrays.asList(header.split("\t"));
    for (int j = 0; j < headerList.size(); j++){
      String tru = truncate(headerList.get(j),10);
      tb.add(tru, String.class);
    }

    ds.createSchema(tb.buildFeatureType());
    ds.setCharset(Charset.forName("GBK"));

    //ReferencingObjectFactory refFactory = new ReferencingObjectFactory();

    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

    for (int i = 0; i < geometries.size(); i++) {
      SimpleFeature feature = writer.next();
      feature.setAttribute("the_geom", geometries.get(i));
      feature.setAttribute("outPolyID", i);
      Envelope env = geometries.get(i).getEnvelopeInternal();
      feature.setAttribute("minLat", env.getMinY());
      feature.setAttribute("minLon", env.getMinX());
      feature.setAttribute("maxLat", env.getMaxY());
      feature.setAttribute("maxLon", env.getMaxX());
//      System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+ geometries.get(i).getUserData());
//      feature.setAttribute(header, geometries.get(i).getUserData());

      List<String> usrData = Arrays.asList(geometries.get(i).getUserData().toString().split("\t"));
      for (int k = 0; k < usrData.size(); k++){
        if (usrData.get(k) != null)
          feature.setAttribute(headerName.get(k), usrData.get(k));
        else
          feature.setAttribute(headerName.get(k), " ");
      }

      writer.write();
    }
    writer.close();
    ds.dispose();


    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop writing shapefile ******************Took " + (endTime - startTime) / 1000 + "s");
  }

  public static void saveAsGeoJSON(String filepath, List<Geometry> geometries, String crs)
      throws IOException {

    System.out.println(
            "*****************start writing jsonfile ******************");
    Long startTime = System.currentTimeMillis();
    GeoJSONWriter geoJSONWriter = new GeoJSONWriter();
    Feature[] features = new Feature[geometries.size()];
    int i = 0;
    for (Geometry geometry : geometries) {
      if (geometry.getUserData() != null) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("UserData", geometry.getUserData());
        features[i++] = new Feature(geoJSONWriter.write(geometry), map);
      } else {
        features[i++] = new Feature(geoJSONWriter.write(geometry), null);
      }
    }

    FeatureCollection featureCollection = new FeatureCollection(features);

    String geojsonStr = featureCollection.toString();
    FileUtils.writeStringToFile(new File(filepath), geojsonStr);

    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop writing jsonfile ******************Took " + (endTime - startTime) / 1000 + "s");
  }

  public static String geojson2shp(String jsonFolder, String shpFolder, String crs){

    System.out.println(
            "*****************start convert geojsonfile to shapefile******************");
    Long startTime = System.currentTimeMillis();

    String shpPath = "";
    GeometryJSON gjson = new GeometryJSON();
    try{
      //create shape file
      File shpfolder = new File(shpFolder);
      shpfolder.mkdir();
      shpPath = shpfolder.getPath() + "/" + shpfolder.getName() + ".shp";
      File file = new File(shpPath);

      Map<String, Serializable> params = new HashMap<String, Serializable>();
      params.put( ShapefileDataStoreFactory.URLP.key, file.toURI().toURL() );
      ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

      //define attribute
      SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
      CoordinateReferenceSystem crsType = CRS.decode(crs);
      tb.setCRS(crsType);

      tb.setName("shapefile");
      tb.add("the_geom", Polygon.class);
      tb.add("POIID", Long.class);
      ds.createSchema(tb.buildFeatureType());
      //set code
      Charset charset = Charset.forName("GBK");
      ds.setCharset(charset);
      //set writer
      FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

      File folder = new File(jsonFolder);
      File[] listOfFiles = folder.listFiles();
      int id = 0;
      for (int j = 0; j < listOfFiles.length; j++) {
        if (listOfFiles[j].isFile()) {
          String filePath = listOfFiles[j].getPath();
          String fileName = listOfFiles[j].getName();
          if(fileName.startsWith("_") || fileName.startsWith(".")){
            continue;
          }
          String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
          JSONObject json = new JSONObject(strJson);
          JSONArray features = (JSONArray) json.get("features");
          for (int i = 0, len = features.length(); i < len; i++) {
            String strFeature = features.getString(i);
            Reader reader = new StringReader(strFeature);
            Geometry geometry = gjson.read(reader);
            SimpleFeature feature = writer.next();
            feature.setAttribute("the_geom", geometry);
            feature.setAttribute("POIID", id);
            id += 1;
            writer.write();
          }
        }
      }

      System.out.println( id + " polygons");
      writer.close();
      ds.dispose();
    }
    catch(Exception e){
      e.printStackTrace();
    }

    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop converting geojsonfile to shapefile ******************Took " + (endTime - startTime) / 1000 + "s");

    return shpPath;
  }

  public static void main(String[] args) {
    String localGeoJsonFolder = "/Users/feihu/Desktop/GeoJson";
    String shpFolder = "/Users/feihu/Desktop/shp";
    String crs = "epsg:4326";
    GeometryReaderUtil_Header.geojson2shp(localGeoJsonFolder, shpFolder, crs);

  }
}


