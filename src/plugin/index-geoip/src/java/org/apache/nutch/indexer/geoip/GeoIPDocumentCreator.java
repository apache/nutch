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
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AbstractCityResponse;
import com.maxmind.geoip2.model.AbstractCountryResponse;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
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
 * Simple utility class which builds a
 * {@link org.apache.nutch.indexer.NutchDocument} based on input from
 * {@link GeoIPIndexingFilter}, where configuration is also read.
 * </p>
 */
public class GeoIPDocumentCreator {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final String NETWORK_ADDRESS = "networkAddress";

  private GeoIPDocumentCreator() {}

  /**
   * Add field to document but only if value isn't null
   * @param doc the {@link NutchDocument} to augment
   * @param name the name of the target field
   * @param value the String value to associate with the target field
   */
  private static void addIfNotNull(NutchDocument doc, String name,
      Object value) {
    if (value != null) {
      doc.add(name, value);
    }
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param reader
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromAnonymousIpDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AnonymousIpResponse> opt = reader.tryAnonymousIp(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AnonymousIpResponse response = opt.get();
      addIfNotNull(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, NETWORK_ADDRESS, response.getNetwork().toString());
      addIfNotNull(doc, "isAnonymous", response.isAnonymous());
      addIfNotNull(doc, "isAnonymousVpn", response.isAnonymousVpn());
      addIfNotNull(doc, "isHostingProxy", response.isHostingProvider());
      addIfNotNull(doc, "isPublicProxy", response.isPublicProxy());
      addIfNotNull(doc, "isResidentialProxy", response.isResidentialProxy());
      addIfNotNull(doc, "isTorExitNode", response.isTorExitNode());
    } else {
      LOG.debug("'{}' IP address not found in Anonymous IP DB.", serverIp);
    }
    return doc;
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param reader
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromAsnDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AsnResponse> opt = reader.tryAsn(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AsnResponse response = opt.get();
      addIfNotNull(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, NETWORK_ADDRESS, response.getNetwork().toString());
      addIfNotNull(doc, "autonomousSystemNumber", response.getAutonomousSystemNumber());
      addIfNotNull(doc, "autonomousSystemOrganization", response.getAutonomousSystemOrganization());
    } else {
      LOG.debug("'{}' IP address not found in ASN DB.", serverIp);
    }
    return doc;
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param reader
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromCityDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    addIfNotNull(doc, "ip", serverIp);
    Optional<CityResponse> opt = reader.tryCity(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      processDocument(doc, opt.get());
    } else {
      LOG.debug("'{}' IP address not found in City DB.", serverIp);
    }
    return doc;
  }

  private static NutchDocument processDocument(NutchDocument doc, AbstractResponse response) {
    City city = ((AbstractCityResponse) response).getCity();
    addIfNotNull(doc, "cityName", city.getName());
    addIfNotNull(doc, "cityConfidence", city.getConfidence());
    addIfNotNull(doc, "cityGeoNameId", city.getGeoNameId());

    Continent continent = ((AbstractCountryResponse) response).getContinent();
    addIfNotNull(doc, "continentCode", continent.getCode());
    addIfNotNull(doc, "continentGeoNameId", continent.getGeoNameId());
    addIfNotNull(doc, "continentName", continent.getName());

    Country country = ((AbstractCountryResponse) response).getRegisteredCountry();
    addIfNotNull(doc, "countryIsoCode", country.getIsoCode());
    addIfNotNull(doc, "countryName", country.getName());
    addIfNotNull(doc, "countryConfidence", country.getConfidence());
    addIfNotNull(doc, "countryGeoNameId", country.getGeoNameId());
    addIfNotNull(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

    Location location = ((AbstractCityResponse) response).getLocation();
    addIfNotNull(doc, "latLon", location.getLatitude() + "," + location.getLongitude());
    addIfNotNull(doc, "accuracyRadius", location.getAccuracyRadius());
    addIfNotNull(doc, "timeZone", location.getTimeZone());
    addIfNotNull(doc, "metroCode", location.getMetroCode());
    addIfNotNull(doc, "populationDensity", location.getPopulationDensity());
    addIfNotNull(doc, "timezone", location.getTimeZone());

    Postal postal = ((AbstractCityResponse) response).getPostal();
    addIfNotNull(doc, "postalCode", postal.getCode());
    addIfNotNull(doc, "postalConfidence", postal.getConfidence());

    RepresentedCountry rCountry = ((AbstractCountryResponse) response).getRepresentedCountry();
    addIfNotNull(doc, "countryType", rCountry.getType());

    Subdivision mostSubdivision = ((AbstractCityResponse) response).getMostSpecificSubdivision();
    addIfNotNull(doc, "mostSpecificSubDivName", mostSubdivision.getName());
    addIfNotNull(doc, "mostSpecificSubDivIsoCode", mostSubdivision.getIsoCode());
    addIfNotNull(doc, "mostSpecificSubDivConfidence", mostSubdivision.getConfidence());
    addIfNotNull(doc, "mostSpecificSubDivGeoNameId", mostSubdivision.getGeoNameId());

    Subdivision leastSubdivision = ((AbstractCityResponse) response).getLeastSpecificSubdivision();
    addIfNotNull(doc, "leastSpecificSubDivName", leastSubdivision.getName());
    addIfNotNull(doc, "leastSpecificSubDivIsoCode", leastSubdivision.getIsoCode());
    addIfNotNull(doc, "leastSpecificSubDivConfidence", leastSubdivision.getConfidence());
    addIfNotNull(doc, "leastSpecificSubDivGeoNameId", leastSubdivision.getGeoNameId());

    Traits traits = ((AbstractCountryResponse) response).getTraits();
    addIfNotNull(doc, "autonomousSystemNumber", traits.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonomousSystemOrganization", traits.getAutonomousSystemOrganization());
    addIfNotNull(doc, "connectionType", traits.getConnectionType().toString());
    addIfNotNull(doc, "domain", traits.getDomain());
    addIfNotNull(doc, "isp", traits.getIsp());
    addIfNotNull(doc, "mobileCountryCode", traits.getMobileCountryCode());
    addIfNotNull(doc, "mobileNetworkCode", traits.getMobileNetworkCode());
    addIfNotNull(doc, NETWORK_ADDRESS, traits.getNetwork().toString());
    addIfNotNull(doc, "organization", traits.getOrganization());
    addIfNotNull(doc, "staticIpScore", traits.getStaticIpScore());
    addIfNotNull(doc, "userCount", traits.getUserCount());
    addIfNotNull(doc, "userType", traits.getUserType());
    addIfNotNull(doc, "isAnonymous", traits.isAnonymous());
    addIfNotNull(doc, "isAnonymousVpn", traits.isAnonymousVpn());
    addIfNotNull(doc, "isAnycast", traits.isAnycast());
    addIfNotNull(doc, "isHostingProvider", traits.isHostingProvider());
    addIfNotNull(doc, "isLegitimateProxy", traits.isLegitimateProxy());
    addIfNotNull(doc, "isPublicProxy", traits.isPublicProxy());
    addIfNotNull(doc, "isResidentialProxy", traits.isResidentialProxy());
    addIfNotNull(doc, "isTorExitNode", traits.isTorExitNode());
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in ConnectionDb.
   * @param serverIp the server IP
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws UnknownHostException if IP address of host could not be determined
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromConnectionDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<ConnectionTypeResponse> opt = reader.tryConnectionType(InetAddress
        .getByName(serverIp));
    if (opt.isPresent()) {
      ConnectionTypeResponse response = opt.get();
      addIfNotNull(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, "connectionType", response.getConnectionType().toString());
      addIfNotNull(doc, NETWORK_ADDRESS, response.getNetwork().toString());
    } else {
      LOG.debug("'{}' IP address not found in Connection DB.", serverIp);
    }
    return doc;
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param reader
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromDomainDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<DomainResponse> opt = reader.tryDomain(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      DomainResponse response = opt.get();
      addIfNotNull(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, "domain", response.getDomain());
      addIfNotNull(doc, NETWORK_ADDRESS, response.getNetwork().toString());
    } else {
      LOG.debug("'{}' IP address not found in Domain DB.", serverIp);
    }
    return doc;
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param client
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromInsightsService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws IOException, GeoIp2Exception {
    addIfNotNull(doc, "ip", serverIp);
    return processDocument(doc, client.insights(InetAddress.getByName(serverIp)));
  }

  /**
   * 
   * @param serverIp
   * @param doc
   * @param reader
   * @return
   * @throws IOException
   * @throws GeoIp2Exception
   */
  public static NutchDocument createDocFromIspDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    IspResponse response = reader.isp(InetAddress.getByName(serverIp));
    addIfNotNull(doc, "ip", serverIp);
    addIfNotNull(doc, "autonSystemNum", response.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonSystemOrg", response.getAutonomousSystemOrganization());
    addIfNotNull(doc, "isp", response.getIsp());
    addIfNotNull(doc, "org", response.getOrganization());
    return doc;
  }

}
