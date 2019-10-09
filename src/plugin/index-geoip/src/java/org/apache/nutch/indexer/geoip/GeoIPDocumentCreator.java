/*
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
 * <p>
 * Simple utility class which enables efficient, structured
 * {@link org.apache.nutch.indexer.NutchDocument} building based on input from
 * {@link GeoIPIndexingFilter}, where configuration is also read.
 * </p>
 * <p>
 * Based on the nature of the input, this class wraps factory type
 * implementations for populating {@link org.apache.nutch.indexer.NutchDocument}
 * 's with the correct {@link org.apache.nutch.indexer.NutchField} information.
 * 
 */
public class GeoIPDocumentCreator {

  /** Add field to document but only if value isn't null */
  public static void addIfNotNull(NutchDocument doc, String name,
      String value) {
    if (value != null) {
      doc.add(name, value);
    }
  }

  /** Add field to document but only if value isn't null */
  public static void addIfNotNull(NutchDocument doc, String name,
      Integer value) {
    if (value != null) {
      doc.add(name, value);
    }
  }

  public static NutchDocument createDocFromInsightsService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException,
      IOException, GeoIp2Exception {
    addIfNotNull(doc, "ip", serverIp);
    InsightsResponse response = client
        .insights(InetAddress.getByName(serverIp));
    // CityResponse response = client.city(InetAddress.getByName(serverIp));

    City city = response.getCity();
    addIfNotNull(doc, "cityName", city.getName()); // 'Minneapolis'
    addIfNotNull(doc, "cityConfidence", city.getConfidence()); // 50
    addIfNotNull(doc, "cityGeoNameId", city.getGeoNameId());

    Continent continent = response.getContinent();
    addIfNotNull(doc, "continentCode", continent.getCode());
    addIfNotNull(doc, "continentGeoNameId", continent.getGeoNameId());
    addIfNotNull(doc, "continentName", continent.getName());

    Country country = response.getCountry();
    addIfNotNull(doc, "countryIsoCode", country.getIsoCode()); // 'US'
    addIfNotNull(doc, "countryName", country.getName()); // 'United States'
    addIfNotNull(doc, "countryConfidence", country.getConfidence()); // 99
    addIfNotNull(doc, "countryGeoName", country.getGeoNameId());

    Location location = response.getLocation();
    addIfNotNull(doc, "latLon", location.getLatitude() + "," + location.getLongitude()); // 44.9733,
                                                                               // -93.2323
    addIfNotNull(doc, "accRadius", location.getAccuracyRadius()); // 3
    addIfNotNull(doc, "timeZone", location.getTimeZone()); // 'America/Chicago'
    addIfNotNull(doc, "metroCode", location.getMetroCode());

    Postal postal = response.getPostal();
    addIfNotNull(doc, "postalCode", postal.getCode()); // '55455'
    addIfNotNull(doc, "postalConfidence", postal.getConfidence()); // 40

    RepresentedCountry rCountry = response.getRepresentedCountry();
    addIfNotNull(doc, "countryType", rCountry.getType());

    Subdivision subdivision = response.getMostSpecificSubdivision();
    addIfNotNull(doc, "subDivName", subdivision.getName()); // 'Minnesota'
    addIfNotNull(doc, "subDivIdoCode", subdivision.getIsoCode()); // 'MN'
    addIfNotNull(doc, "subDivConfidence", subdivision.getConfidence()); // 90
    addIfNotNull(doc, "subDivGeoNameId", subdivision.getGeoNameId());

    Traits traits = response.getTraits();
    addIfNotNull(doc, "autonSystemNum", traits.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonSystemOrg", traits.getAutonomousSystemOrganization());
    addIfNotNull(doc, "domain", traits.getDomain());
    addIfNotNull(doc, "isp", traits.getIsp());
    addIfNotNull(doc, "org", traits.getOrganization());
    addIfNotNull(doc, "userType", traits.getUserType());
    //for better results, users should upgrade to
    //https://www.maxmind.com/en/solutions/geoip2-enterprise-product-suite/anonymous-ip-database
    addIfNotNull(doc, "isAnonProxy", String.valueOf(traits.isAnonymousProxy()));
    return doc;
  }

  @SuppressWarnings("unused")
  public static NutchDocument createDocFromCityService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException,
      IOException, GeoIp2Exception {
    CityResponse response = client.city(InetAddress.getByName(serverIp));
    return doc;
  }

  @SuppressWarnings("unused")
  public static NutchDocument createDocFromCountryService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws UnknownHostException,
      IOException, GeoIp2Exception {
    CountryResponse response = client.country(InetAddress.getByName(serverIp));
    return doc;
  }

  public static NutchDocument createDocFromIspDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws UnknownHostException,
      IOException, GeoIp2Exception {
    IspResponse response = reader.isp(InetAddress.getByName(serverIp));
    addIfNotNull(doc, "ip", serverIp);
    addIfNotNull(doc, "autonSystemNum", response.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonSystemOrg", response.getAutonomousSystemOrganization());
    addIfNotNull(doc, "isp", response.getIsp());
    addIfNotNull(doc, "org", response.getOrganization());
    return doc;
  }

  public static NutchDocument createDocFromDomainDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws UnknownHostException,
      IOException, GeoIp2Exception {
    DomainResponse response = reader.domain(InetAddress.getByName(serverIp));
    addIfNotNull(doc, "ip", serverIp);
    addIfNotNull(doc, "domain", response.getDomain());
    return doc;
  }

  public static NutchDocument createDocFromConnectionDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws UnknownHostException,
      IOException, GeoIp2Exception {
    ConnectionTypeResponse response = reader.connectionType(InetAddress
        .getByName(serverIp));
    addIfNotNull(doc, "ip", serverIp);
    addIfNotNull(doc, "connType", response.getConnectionType().toString());
    return doc;
  }

  public static NutchDocument createDocFromCityDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws UnknownHostException,
      IOException, GeoIp2Exception {
    addIfNotNull(doc, "ip", serverIp);
    CityResponse response = reader.city(InetAddress.getByName(serverIp));

    City city = response.getCity();
    addIfNotNull(doc, "cityName", city.getName()); // 'Minneapolis'
    addIfNotNull(doc, "cityConfidence", city.getConfidence()); // 50
    addIfNotNull(doc, "cityGeoNameId", city.getGeoNameId());


    Continent continent = response.getContinent();
    addIfNotNull(doc, "continentCode", continent.getCode());
    addIfNotNull(doc, "continentGeoNameId", continent.getGeoNameId());
    addIfNotNull(doc, "continentName", continent.getName());

    Country country = response.getCountry();
    addIfNotNull(doc, "countryIsoCode", country.getIsoCode()); // 'US'
    addIfNotNull(doc, "countryName", country.getName()); // 'United States'
    addIfNotNull(doc, "countryConfidence", country.getConfidence()); // 99
    addIfNotNull(doc, "countryGeoName", country.getGeoNameId());

    Location location = response.getLocation();
    addIfNotNull(doc, "latLon", location.getLatitude() + "," + location.getLongitude()); // 44.9733,
                                                                               // -93.2323
    addIfNotNull(doc, "accRadius", location.getAccuracyRadius()); // 3
    addIfNotNull(doc, "timeZone", location.getTimeZone()); // 'America/Chicago'
    addIfNotNull(doc, "metroCode", location.getMetroCode());

    Postal postal = response.getPostal();
    addIfNotNull(doc, "postalCode", postal.getCode()); // '55455'
    addIfNotNull(doc, "postalConfidence", postal.getConfidence()); // 40

    RepresentedCountry rCountry = response.getRepresentedCountry();
    addIfNotNull(doc, "countryType", rCountry.getType());

    Subdivision subdivision = response.getMostSpecificSubdivision();
    addIfNotNull(doc, "subDivName", subdivision.getName()); // 'Minnesota'
    addIfNotNull(doc, "subDivIdoCode", subdivision.getIsoCode()); // 'MN'
    addIfNotNull(doc, "subDivConfidence", subdivision.getConfidence()); // 90
    addIfNotNull(doc, "subDivGeoNameId", subdivision.getGeoNameId());
    return doc;
  }

}
