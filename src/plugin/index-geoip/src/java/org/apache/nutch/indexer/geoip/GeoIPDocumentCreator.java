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
import java.util.Optional;

import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.InsightsResponse;
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

  // Database-specific network address field names
  static final String ANONYMOUS_NETWORK_ADDRESS = "anonymousNetworkAddress";
  static final String ASN_NETWORK_ADDRESS = "asnNetworkAddress";
  static final String CITY_NETWORK_ADDRESS = "cityNetworkAddress";
  static final String CONNECTION_NETWORK_ADDRESS = "connectionNetworkAddress";
  static final String COUNTRY_NETWORK_ADDRESS = "countryNetworkAddress";
  static final String DOMAIN_NETWORK_ADDRESS = "domainNetworkAddress";
  static final String INSIGHTS_NETWORK_ADDRESS = "insightsNetworkAddress";

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
   * Add field to document only if the field doesn't already contain this exact value.
   * Use for fields like "ip" that should not have duplicate values.
   * @param doc the {@link NutchDocument} to augment
   * @param name the name of the target field
   * @param value the value to associate with the target field
   */
  static void addIfNotDuplicate(NutchDocument doc, String name, Object value) {
    if (value == null) {
      return;
    }
    NutchField field = doc.getField(name);
    if (field == null || !field.getValues().contains(value)) {
      doc.add(name, value);
    }
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Anonymous IP database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromAnonymousIpDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AnonymousIpResponse> opt = reader.tryAnonymousIp(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AnonymousIpResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, ANONYMOUS_NETWORK_ADDRESS, response.getNetwork().toString());
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
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in ASN database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromAsnDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<AsnResponse> opt = reader.tryAsn(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      AsnResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, ASN_NETWORK_ADDRESS, response.getNetwork().toString());
      addIfNotNull(doc, "autonomousSystemNumber", response.getAutonomousSystemNumber());
      addIfNotNull(doc, "autonomousSystemOrganization", response.getAutonomousSystemOrganization());
    } else {
      LOG.debug("'{}' IP address not found in ASN DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in City database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromCityDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    addIfNotDuplicate(doc, "ip", serverIp);
    Optional<CityResponse> opt = reader.tryCity(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      processCityDocument(doc, opt.get());
    } else {
      LOG.debug("'{}' IP address not found in City DB.", serverIp);
    }
    return doc;
  }

  private static NutchDocument processCityDocument(NutchDocument doc, CityResponse response) {
    City city = response.getCity();
    addIfNotNull(doc, "cityName", city.getName());
    addIfNotNull(doc, "cityConfidence", city.getConfidence());
    addIfNotNull(doc, "cityGeoNameId", city.getGeoNameId());

    Continent continent = response.getContinent();
    addIfNotNull(doc, "continentCode", continent.getCode());
    addIfNotNull(doc, "continentGeoNameId", continent.getGeoNameId());
    addIfNotNull(doc, "continentName", continent.getName());

    Country country = response.getRegisteredCountry();
    addIfNotNull(doc, "countryIsoCode", country.getIsoCode());
    addIfNotNull(doc, "countryName", country.getName());
    addIfNotNull(doc, "countryConfidence", country.getConfidence());
    addIfNotNull(doc, "countryGeoNameId", country.getGeoNameId());
    addIfNotNull(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

    Location location = response.getLocation();
    if (location.getLatitude() != null && location.getLongitude() != null) {
      addIfNotNull(doc, "latLon", location.getLatitude() + "," + location.getLongitude());
    }
    addIfNotNull(doc, "accuracyRadius", location.getAccuracyRadius());
    addIfNotNull(doc, "timeZone", location.getTimeZone());
    addIfNotNull(doc, "populationDensity", location.getPopulationDensity());

    Postal postal = response.getPostal();
    addIfNotNull(doc, "postalCode", postal.getCode());
    addIfNotNull(doc, "postalConfidence", postal.getConfidence());

    RepresentedCountry rCountry = response.getRepresentedCountry();
    addIfNotNull(doc, "countryType", rCountry.getType());

    Subdivision mostSubdivision = response.getMostSpecificSubdivision();
    addIfNotNull(doc, "mostSpecificSubDivName", mostSubdivision.getName());
    addIfNotNull(doc, "mostSpecificSubDivIsoCode", mostSubdivision.getIsoCode());
    addIfNotNull(doc, "mostSpecificSubDivConfidence", mostSubdivision.getConfidence());
    addIfNotNull(doc, "mostSpecificSubDivGeoNameId", mostSubdivision.getGeoNameId());

    Subdivision leastSubdivision = response.getLeastSpecificSubdivision();
    addIfNotNull(doc, "leastSpecificSubDivName", leastSubdivision.getName());
    addIfNotNull(doc, "leastSpecificSubDivIsoCode", leastSubdivision.getIsoCode());
    addIfNotNull(doc, "leastSpecificSubDivConfidence", leastSubdivision.getConfidence());
    addIfNotNull(doc, "leastSpecificSubDivGeoNameId", leastSubdivision.getGeoNameId());

    Traits traits = response.getTraits();
    addIfNotNull(doc, "autonomousSystemNumber", traits.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonomousSystemOrganization", traits.getAutonomousSystemOrganization());
    if (traits.getConnectionType() != null) {
      addIfNotNull(doc, "connectionType", traits.getConnectionType().toString());
    }
    addIfNotNull(doc, "domain", traits.getDomain());
    addIfNotNull(doc, "isp", traits.getIsp());
    addIfNotNull(doc, "mobileCountryCode", traits.getMobileCountryCode());
    addIfNotNull(doc, "mobileNetworkCode", traits.getMobileNetworkCode());
    if (traits.getNetwork() != null) {
      addIfNotNull(doc, CITY_NETWORK_ADDRESS, traits.getNetwork().toString());
    }
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
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromConnectionDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<ConnectionTypeResponse> opt = reader.tryConnectionType(InetAddress
        .getByName(serverIp));
    if (opt.isPresent()) {
      ConnectionTypeResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.getIpAddress());
      if (response.getConnectionType() != null) {
        addIfNotNull(doc, "connectionType", response.getConnectionType().toString());
      }
      if (response.getNetwork() != null) {
        addIfNotNull(doc, CONNECTION_NETWORK_ADDRESS, response.getNetwork().toString());
      }
    } else {
      LOG.debug("'{}' IP address not found in Connection DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Country database. This is a lighter-weight alternative to the City
   * database when only country-level information is needed.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromCountryDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<CountryResponse> opt = reader.tryCountry(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      CountryResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", serverIp);

      Continent continent = response.getContinent();
      addIfNotDuplicate(doc, "continentCode", continent.getCode());
      addIfNotDuplicate(doc, "continentGeoNameId", continent.getGeoNameId());
      addIfNotDuplicate(doc, "continentName", continent.getName());

      Country country = response.getRegisteredCountry();
      addIfNotDuplicate(doc, "countryIsoCode", country.getIsoCode());
      addIfNotDuplicate(doc, "countryName", country.getName());
      addIfNotDuplicate(doc, "countryConfidence", country.getConfidence());
      addIfNotDuplicate(doc, "countryGeoNameId", country.getGeoNameId());
      addIfNotDuplicate(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

      RepresentedCountry rCountry = response.getRepresentedCountry();
      addIfNotDuplicate(doc, "countryType", rCountry.getType());

      Traits traits = response.getTraits();
      if (traits.getNetwork() != null) {
        addIfNotNull(doc, COUNTRY_NETWORK_ADDRESS, traits.getNetwork().toString());
      }
      addIfNotDuplicate(doc, "isAnonymous", traits.isAnonymous());
      addIfNotDuplicate(doc, "isAnonymousVpn", traits.isAnonymousVpn());
      addIfNotDuplicate(doc, "isAnycast", traits.isAnycast());
      addIfNotDuplicate(doc, "isHostingProvider", traits.isHostingProvider());
      addIfNotDuplicate(doc, "isLegitimateProxy", traits.isLegitimateProxy());
      addIfNotDuplicate(doc, "isPublicProxy", traits.isPublicProxy());
      addIfNotDuplicate(doc, "isResidentialProxy", traits.isResidentialProxy());
      addIfNotDuplicate(doc, "isTorExitNode", traits.isTorExitNode());
    } else {
      LOG.debug("'{}' IP address not found in Country DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP in Domain database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromDomainDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<DomainResponse> opt = reader.tryDomain(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      DomainResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, "domain", response.getDomain());
      addIfNotNull(doc, DOMAIN_NETWORK_ADDRESS, response.getNetwork().toString());
    } else {
      LOG.debug("'{}' IP address not found in Domain DB.", serverIp);
    }
    return doc;
  }

  /**
   * Populate a {@link org.apache.nutch.indexer.NutchDocument} based on lookup
   * of IP using the MaxMind Insights web service.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param client WebServiceClient for MaxMind Insights API
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromInsightsService(String serverIp,
      NutchDocument doc, WebServiceClient client) throws IOException, GeoIp2Exception {
    addIfNotDuplicate(doc, "ip", serverIp);
    return processInsightsDocument(doc, client.insights(InetAddress.getByName(serverIp)));
  }

  private static NutchDocument processInsightsDocument(NutchDocument doc, InsightsResponse response) {
    City city = response.getCity();
    addIfNotNull(doc, "cityName", city.getName());
    addIfNotNull(doc, "cityConfidence", city.getConfidence());
    addIfNotNull(doc, "cityGeoNameId", city.getGeoNameId());

    Continent continent = response.getContinent();
    addIfNotNull(doc, "continentCode", continent.getCode());
    addIfNotNull(doc, "continentGeoNameId", continent.getGeoNameId());
    addIfNotNull(doc, "continentName", continent.getName());

    Country country = response.getRegisteredCountry();
    addIfNotNull(doc, "countryIsoCode", country.getIsoCode());
    addIfNotNull(doc, "countryName", country.getName());
    addIfNotNull(doc, "countryConfidence", country.getConfidence());
    addIfNotNull(doc, "countryGeoNameId", country.getGeoNameId());
    addIfNotNull(doc, "countryInEuropeanUnion", country.isInEuropeanUnion());

    Location location = response.getLocation();
    if (location.getLatitude() != null && location.getLongitude() != null) {
      addIfNotNull(doc, "latLon", location.getLatitude() + "," + location.getLongitude());
    }
    addIfNotNull(doc, "accuracyRadius", location.getAccuracyRadius());
    addIfNotNull(doc, "timeZone", location.getTimeZone());
    addIfNotNull(doc, "populationDensity", location.getPopulationDensity());

    Postal postal = response.getPostal();
    addIfNotNull(doc, "postalCode", postal.getCode());
    addIfNotNull(doc, "postalConfidence", postal.getConfidence());

    RepresentedCountry rCountry = response.getRepresentedCountry();
    addIfNotNull(doc, "countryType", rCountry.getType());

    Subdivision mostSubdivision = response.getMostSpecificSubdivision();
    addIfNotNull(doc, "mostSpecificSubDivName", mostSubdivision.getName());
    addIfNotNull(doc, "mostSpecificSubDivIsoCode", mostSubdivision.getIsoCode());
    addIfNotNull(doc, "mostSpecificSubDivConfidence", mostSubdivision.getConfidence());
    addIfNotNull(doc, "mostSpecificSubDivGeoNameId", mostSubdivision.getGeoNameId());

    Subdivision leastSubdivision = response.getLeastSpecificSubdivision();
    addIfNotNull(doc, "leastSpecificSubDivName", leastSubdivision.getName());
    addIfNotNull(doc, "leastSpecificSubDivIsoCode", leastSubdivision.getIsoCode());
    addIfNotNull(doc, "leastSpecificSubDivConfidence", leastSubdivision.getConfidence());
    addIfNotNull(doc, "leastSpecificSubDivGeoNameId", leastSubdivision.getGeoNameId());

    Traits traits = response.getTraits();
    addIfNotNull(doc, "autonomousSystemNumber", traits.getAutonomousSystemNumber());
    addIfNotNull(doc, "autonomousSystemOrganization", traits.getAutonomousSystemOrganization());
    if (traits.getConnectionType() != null) {
      addIfNotNull(doc, "connectionType", traits.getConnectionType().toString());
    }
    addIfNotNull(doc, "domain", traits.getDomain());
    addIfNotNull(doc, "isp", traits.getIsp());
    addIfNotNull(doc, "mobileCountryCode", traits.getMobileCountryCode());
    addIfNotNull(doc, "mobileNetworkCode", traits.getMobileNetworkCode());
    if (traits.getNetwork() != null) {
      addIfNotNull(doc, INSIGHTS_NETWORK_ADDRESS, traits.getNetwork().toString());
    }
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
   * of IP in ISP database.
   * @param serverIp the server IP address to lookup
   * @param doc NutchDocument to populate
   * @param reader instantiated DatabaseReader object
   * @return populated NutchDocument
   * @throws IOException if an error occurs performing the Db lookup
   * @throws GeoIp2Exception generic GeoIp2 exception
   */
  public static NutchDocument createDocFromIspDb(String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    Optional<IspResponse> opt = reader.tryIsp(InetAddress.getByName(serverIp));
    if (opt.isPresent()) {
      IspResponse response = opt.get();
      addIfNotDuplicate(doc, "ip", response.getIpAddress());
      addIfNotNull(doc, "autonSystemNum", response.getAutonomousSystemNumber());
      addIfNotNull(doc, "autonSystemOrg", response.getAutonomousSystemOrganization());
      addIfNotNull(doc, "isp", response.getIsp());
      addIfNotNull(doc, "org", response.getOrganization());
    } else {
      LOG.debug("'{}' IP address not found in ISP DB.", serverIp);
    }
    return doc;
  }

}
