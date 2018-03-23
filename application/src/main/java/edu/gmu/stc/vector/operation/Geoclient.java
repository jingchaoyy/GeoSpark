package edu.gmu.stc.vector.operation;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.httpclient.NameValuePair;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher.UploadMethod;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;

/*
 * Geoserver manager: https://github.com/geosolutions-it/geoserver-manager/wiki/Various-Examples
 * How to do it through REST in postman: https://gis.stackexchange.com/questions/12970/create-a-layer-in-geoserver-using-rest
 * http://docs.geoserver.org/latest/en/user/gettingstarted/shapefile-quickstart/index.html
 * 1. Upload data (any local file is fine, zip)
 * 2. Create workspace (can be done manually)
 * 3. Create data store (optional)
 * 4. Publish layer
 */
public class Geoclient {

  private String restURL/*= "http://199.26.254.146:8080/geoserver"*/;
    private String restUser;
    private String restPwd;
  GeoServerRESTReader reader;
  GeoServerRESTPublisher publisher;

  public Geoclient(String restURL, String restUser, String restPwd) {
    this.restURL = restURL;
    this.restUser = restUser;
    this.restPwd = restPwd;
    try {
      reader = new GeoServerRESTReader(restURL, restUser, restPwd);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    publisher = new GeoServerRESTPublisher(restURL, restUser, restPwd);
  }

  public boolean createWorkspace(String workspace_name)
  {
    return publisher.createWorkspace(workspace_name);
  }

  /*
   * WMS_url_pattern:
   * http://localhost:8080/geoserver/$workspace_name/wms?service=WMS&version=1.1.0&request=GetMap&layers=$workspace_name:$data_store_name&styles=&bbox=$bbox&width=506&height=768&srs=$proj&format=application/openlayers
   * http://localhost:8080/geoserver/javatest/wms?service=WMS&version=1.1.0&request=GetMap&layers=javatest:nyc_roads&styles=&bbox=984018.1663741902,207673.09513056703,991906.4970533887,219622.53973435296&width=506&height=768&srs=EPSG:2908&format=application/openlayers
   */
  public String publishShapefile(String workspace_name, String data_store_name, String datasetname,
                                  String remote_data_path, String proj, String bbox)
  {
    boolean bPublished = false;
    String publishedUrl = "";
    //this.createWorkspace(workspace_name);

    URI uri;
    try {
      uri = new URI(remote_data_path);
      bPublished = publisher.publishShp(workspace_name, data_store_name, new NameValuePair[0], datasetname,
              UploadMethod.EXTERNAL, uri, proj, null);
    } catch (Exception e) {
      e.printStackTrace();
      publishedUrl = e.getMessage();
    }

    if(bPublished){
      //publishedUrl = this.restURL  + "/" + workspace_name +"/wms&service=WMS&version=1.1.0&request=GetMap&layers=" + workspace_name + ":" + datasetname +"&styles=&bbox=" + bbox + "&width=506&height=768&srs=" + proj + "&format=application/openlayers";

      //bbox=984018.1663741902,207673.09513056703,991906.4970533887,219622.53973435296
      String[] strCord = bbox.split(",");
      double lonOffset = (Double.parseDouble(strCord[2]) - Double.parseDouble(strCord[0]))/2;
      double latOffset = (Double.parseDouble(strCord[3]) - Double.parseDouble(strCord[1]))/2;
      double centerLon = Double.parseDouble(strCord[0]) + lonOffset;
      double centerLat = Double.parseDouble(strCord[1]) + latOffset;
      String center = Double.toString(centerLon) + "," +  Double.toString(centerLat);

      publishedUrl = this.restURL  + "/" + workspace_name +"/wms|" + workspace_name + ":" + datasetname +"|" + bbox +"|" + center;

    }else{
      publishedUrl = "Failed to publish map." + publishedUrl;
    }
    return publishedUrl;
  }

  public static void main(String[] args) throws URISyntaxException {
      
  }

}