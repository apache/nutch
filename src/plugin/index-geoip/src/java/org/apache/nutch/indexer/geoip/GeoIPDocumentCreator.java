/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer.geoip;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.nutch.indexer.NutchDocument;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.InsightsResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Traits;

/**
 * <p>Simple utility class which enables efficient, structured
 * {@link org.apache.nutch.indexer.NutchDocument} building based on input 
 * from {@link GeoIPIndexingFilter}, where configuration is also read.</p>
 * <p>Based on the nature of the input, this class wraps factory type
 * implementations for populating {@link org.apache.nutch.indexer.NutchDocument}'s
 * with the correct {@link org.apache.nutch.indexer.NutchField} information.
 *
 */
public class GeoIPDocumentCreator {

  /**
   * Default constructor.
   */
  public GeoIPDocumentCreator() {
  }

  public static NutchDocument createDocFromInsightsService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException, IOException, GeoIp2Exception {
    doc.add("ip", serverIp);
    InsightsResponse response = client.insights(InetAddress.getByName(serverIp));
    //CityResponse response = client.city(InetAddress.getByName(serverIp));
    
    City city = response.getCity();
    doc.add("cityName", city.getName());       // 'Minneapolis'
    doc.add("cityConfidence", city.getConfidence()); // 50
    doc.add("cityGeoNameId", city.getGeoNameId());

    Continent continent = response.getContinent();
    doc.add("continentCode", continent.getCode());
    doc.add("continentGeoNameId", continent.getGeoNameId());
    doc.add("continentName", continent.getName());

    Country country = response.getCountry();
    doc.add("countryIsoCode", country.getIsoCode());            // 'US'
    doc.add("countryName", country.getName());               // 'United States'
    doc.add("countryConfidence", country.getConfidence());         // 99
    doc.add("countryGeoName", country.getGeoNameId());

    Location location = response.getLocation();
    doc.add("latLon", location.getLatitude() + "," + location.getLongitude());    // 44.9733, -93.2323
    doc.add("accRadius", location.getAccuracyRadius());  // 3
    doc.add("timeZone", location.getTimeZone());        // 'America/Chicago'
    doc.add("metroCode", location.getMetroCode());

    Postal postal = response.getPostal();
    doc.add("postalCode", postal.getCode());       // '55455'
    doc.add("postalConfidence", postal.getConfidence()); // 40

    RepresentedCountry rCountry = response.getRepresentedCountry();
    doc.add("countryType", rCountry.getType());

    Subdivision subdivision = response.getMostSpecificSubdivision();
    doc.add("subDivName", subdivision.getName());       // 'Minnesota'
    doc.add("subDivIdoCode", subdivision.getIsoCode());    // 'MN'
    doc.add("subDivConfidence", subdivision.getConfidence()); // 90
    doc.add("subDivGeoNameId", subdivision.getGeoNameId());

    Traits traits = response.getTraits(); 
    doc.add("autonSystemNum", traits.getAutonomousSystemNumber());
    doc.add("autonSystemOrg", traits.getAutonomousSystemOrganization());
    doc.add("domain", traits.getDomain());
    doc.add("isp", traits.getIsp());
    doc.add("org", traits.getOrganization());
    doc.add("userType", traits.getUserType());
    doc.add("isAnonProxy", traits.isAnonymousProxy());
    doc.add("isSatelliteProv", traits.isSatelliteProvider());
    return doc;
  }

  @SuppressWarnings("unused")
  public static NutchDocument createDocFromCityService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException, IOException, GeoIp2Exception {
    CityResponse response = client.city(InetAddress.getByName(serverIp));
    return doc;
  }

  @SuppressWarnings("unused")
  public static NutchDocument createDocFromCountryService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException, IOException, GeoIp2Exception {
    CountryResponse response = client.country(InetAddress.getByName(serverIp));    
    return doc;
  }

  public static NutchDocument createDocFromIspDb(String serverIp, NutchDocument doc, 
      DatabaseReader reader) throws UnknownHostException, IOException, GeoIp2Exception {
    IspResponse response = reader.isp(InetAddress.getByName(serverIp));
    doc.add("ip", serverIp);
    doc.add("autonSystemNum", response.getAutonomousSystemNumber());
    doc.add("autonSystemOrg", response.getAutonomousSystemOrganization());
    doc.add("isp", response.getIsp());
    doc.add("org", response.getOrganization());
    return doc;
  }

  public static NutchDocument createDocFromDomainDb(String serverIp, NutchDocument doc, 
      DatabaseReader reader) throws UnknownHostException, IOException, GeoIp2Exception {
    DomainResponse response = reader.domain(InetAddress.getByName(serverIp));
    doc.add("ip", serverIp);
    doc.add("domain", response.getDomain());
    return doc;
  }

  public static NutchDocument createDocFromConnectionDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws UnknownHostException, IOException, GeoIp2Exception {
    ConnectionTypeResponse response = reader.connectionType(InetAddress.getByName(serverIp));
    doc.add("ip", serverIp);
    doc.add("connType", response.getConnectionType().toString());
    return doc;
  }

  public static NutchDocument createDocFromCityDb(String serverIp, NutchDocument doc, 
      DatabaseReader reader) throws UnknownHostException, IOException, GeoIp2Exception {
    doc.add("ip", serverIp);
    CityResponse response = reader.city(InetAddress.getByName(serverIp));

    City city = response.getCity();
    doc.add("cityName", city.getName());       // 'Minneapolis'
    doc.add("cityConfidence", city.getConfidence()); // 50
    doc.add("cityGeoNameId", city.getGeoNameId());

    Continent continent = response.getContinent();
    doc.add("continentCode", continent.getCode());
    doc.add("continentGeoNameId", continent.getGeoNameId());
    doc.add("continentName", continent.getName());

    Country country = response.getCountry();
    doc.add("countryIsoCode", country.getIsoCode());            // 'US'
    doc.add("countryName", country.getName());               // 'United States'
    doc.add("countryConfidence", country.getConfidence());         // 99
    doc.add("countryGeoName", country.getGeoNameId());

    Location location = response.getLocation();
    doc.add("latLon", location.getLatitude() + "," + location.getLongitude());    // 44.9733, -93.2323
    doc.add("accRadius", location.getAccuracyRadius());  // 3
    doc.add("timeZone", location.getTimeZone());        // 'America/Chicago'
    doc.add("metroCode", location.getMetroCode());

    Postal postal = response.getPostal();
    doc.add("postalCode", postal.getCode());       // '55455'
    doc.add("postalConfidence", postal.getConfidence()); // 40

    RepresentedCountry rCountry = response.getRepresentedCountry();
    doc.add("countryType", rCountry.getType());

    Subdivision subdivision = response.getMostSpecificSubdivision();
    doc.add("subDivName", subdivision.getName());       // 'Minnesota'
    doc.add("subDivIdoCode", subdivision.getIsoCode());    // 'MN'
    doc.add("subDivConfidence", subdivision.getConfidence()); // 90
    doc.add("subDivGeoNameId", subdivision.getGeoNameId());
    return doc;
  }

}
