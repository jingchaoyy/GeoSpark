package edu.gmu.stc.vector.operation;

import java.io.File;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.util.json.JSONException;
import com.vividsolutions.jts.geom.*;
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileConverter {
    /**
     * convert geojson to shp
     * @param jsonPath
     * @param shpPath
     * @return
     */
    public static String geojson2shp(String jsonFolder, String shpFolder, String crs){
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
                    System.out.println(filePath);
                    String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
                    JSONObject json = new JSONObject(strJson);
                    JSONArray features = (JSONArray) json.get("features");
                    JSONObject feature0 = new JSONObject(features.get(0).toString());
                    for (int i = 0, len = features.length(); i < len; i++) {
                        String strFeature = features.get(i).toString();
                        Reader reader = new StringReader(strFeature);
                        SimpleFeature feature = writer.next();
                        feature.setAttribute("the_geom", gjson.readPolygon(reader));
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

        return shpPath;
    }

    public static String geometry2Shape(String jsonFolder, String shpFolder){
        String shpPath = "";
        GeometryJSON gjson = new GeometryJSON();
        List<Geometry> geometries = new ArrayList<Geometry>();
        try{
            File folder = new File(jsonFolder);
            File[] listOfFiles = folder.listFiles();
            for (int j = 0; j < listOfFiles.length; j++) {
                if (listOfFiles[j].isFile()) {
                    String filePath = listOfFiles[j].getPath();
                    System.out.println(filePath);
                    String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
                    JSONObject json = new JSONObject(strJson);
                    JSONArray features = (JSONArray) json.get("features");
                    for (int i = 0, len = features.length(); i < len; i++) {
                        String strFeature = features.get(i).toString();
                        Reader reader = new StringReader(strFeature);
                        Polygon polygon = gjson.readPolygon(reader);
                        geometries.add((Geometry) polygon);
                    }
                }
            }

            File shpfolder = new File(shpFolder);
            shpfolder.mkdir();
            shpPath = shpfolder.getPath() + "/" + shpfolder.getName() + ".shp";
            GeometryReaderUtil.saveAsShapefile(shpPath, geometries, "epsg:4326");
            } catch (JSONException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (FactoryException e) {
            e.printStackTrace();
        }

        return shpPath;
    }

    public static void main(String[] args){

        long start = System.currentTimeMillis();
        //String shpPath = "/Users/lzugis/Documents/chinadata/cityboundry.shp";
        //String jsonPath = "D:/Desktop/test031612.geojson/part-00000.json";
        //Map map = fileFormat.shape2Geojson(shpPath, jsonPath);

      //String shpPath = "C:/Users/tinay/Desktop/shp032201.shp";
      String shpFolder = "C:/Users/tinay/Desktop/shp032201";
      String jsonFolder = "C:/Users/tinay/Desktop/json032201/";
      String shpPath = FileConverter.geometry2Shape(jsonFolder, shpFolder);

        System.out.println(shpPath+", took "+(System.currentTimeMillis() - start)/1000 +"s");
    }
}