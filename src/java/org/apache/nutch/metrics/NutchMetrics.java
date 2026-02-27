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
package org.apache.nutch.metrics;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.FetchSchedule;

/**
 * Centralized constants for Hadoop metrics counter groups and names.
 * 
 * <p>Follows <a href="https://prometheus.io/docs/practices/naming/">Prometheus
 * naming conventions</a>:
 * <ul>
 *   <li>Counter groups use the {@code nutch_} prefix namespace</li>
 *   <li>Counter names use snake_case</li>
 *   <li>Accumulating counters use {@code _total} suffix</li>
 *   <li>Units are included in counter names where applicable (e.g., {@code _bytes})</li>
 * </ul>
 * 
 * @since 1.22
 */
public final class NutchMetrics {

  private NutchMetrics() {
    // Utility class - prevent instantiation
  }

  // =========================================================================
  // Counter Groups (Prometheus namespace style with nutch_ prefix)
  // =========================================================================

  /** Counter group for fetcher operations. */
  public static final String GROUP_FETCHER = "nutch_fetcher";

  /** Counter group for fetcher outlink processing. */
  public static final String GROUP_FETCHER_OUTLINKS = "nutch_fetcher_outlinks";

  /** Counter group for generator operations. */
  public static final String GROUP_GENERATOR = "nutch_generator";

  /** Counter group for indexer operations. */
  public static final String GROUP_INDEXER = "nutch_indexer";

  /** Counter group for CrawlDb operations. */
  public static final String GROUP_CRAWLDB = "nutch_crawldb";

  /** Counter group for CrawlDb filter operations. */
  public static final String GROUP_CRAWLDB_FILTER = "nutch_crawldb_filter";

  /** Counter group for injector operations. */
  public static final String GROUP_INJECTOR = "nutch_injector";

  /** Counter group for HostDb operations. */
  public static final String GROUP_HOSTDB = "nutch_hostdb";

  /** Counter group for parser operations. */
  public static final String GROUP_PARSER = "nutch_parser";

  /** Counter group for deduplication operations. */
  public static final String GROUP_DEDUP = "nutch_dedup";

  /** Counter group for cleaning job operations. */
  public static final String GROUP_CLEANING = "nutch_cleaning";

  /** Counter group for WebGraph operations. */
  public static final String GROUP_WEBGRAPH = "nutch_webgraph";

  /** Counter group for sitemap processing operations. */
  public static final String GROUP_SITEMAP = "nutch_sitemap";

  /** Counter group for WARC export operations. */
  public static final String GROUP_WARC_EXPORTER = "nutch_warc_exporter";

  /** Counter group for domain statistics operations. */
  public static final String GROUP_DOMAIN_STATS = "nutch_domain_stats";

  // =========================================================================
  // Fetcher Counters
  // =========================================================================

  /** Total bytes downloaded by fetcher. */
  public static final String FETCHER_BYTES_DOWNLOADED_TOTAL = "bytes_downloaded_total";

  /** URLs denied by robots.txt. */
  public static final String FETCHER_ROBOTS_DENIED_TOTAL = "robots_denied_total";

  /** URLs denied due to crawl delay exceeding maximum. */
  public static final String FETCHER_ROBOTS_DENIED_MAXCRAWLDELAY_TOTAL = "robots_denied_maxcrawldelay_total";

  /** URLs dropped due to robots.txt deferred visits. */
  public static final String FETCHER_ROBOTS_DEFER_VISITS_DROPPED_TOTAL = "robots_defer_visits_dropped_total";

  /** Redirects that exceeded maximum redirect count. */
  public static final String FETCHER_REDIRECT_COUNT_EXCEEDED_TOTAL = "redirect_count_exceeded_total";

  /** Redirects deduplicated (already seen). */
  public static final String FETCHER_REDIRECT_DEDUPLICATED_TOTAL = "redirect_deduplicated_total";

  /** FetchItems not created for redirects. */
  public static final String FETCHER_REDIRECT_NOT_CREATED_TOTAL = "redirect_not_created_total";

  /** URLs hit by time limit. */
  public static final String FETCHER_HIT_BY_TIMELIMIT_TOTAL = "hit_by_timelimit_total";

  /** URLs hit by timeout. */
  public static final String FETCHER_HIT_BY_TIMEOUT_TOTAL = "hit_by_timeout_total";

  /** URLs hit by throughput threshold. */
  public static final String FETCHER_HIT_BY_THROUGHPUT_THRESHOLD_TOTAL = "hit_by_throughput_threshold_total";

  /** Threads that hung during fetching. */
  public static final String FETCHER_HUNG_THREADS_TOTAL = "hung_threads_total";

  /** URLs filtered during fetching. */
  public static final String FETCHER_FILTERED_TOTAL = "filtered_total";

  /** URLs dropped due to exception threshold in queue. */
  public static final String FETCHER_ABOVE_EXCEPTION_THRESHOLD_TOTAL = "above_exception_threshold_total";

  // =========================================================================
  // Fetcher Outlinks Counters
  // =========================================================================

  /** Outlinks detected during parsing. */
  public static final String FETCHER_OUTLINKS_DETECTED_TOTAL = "outlinks_detected_total";

  /** Outlinks being followed. */
  public static final String FETCHER_OUTLINKS_FOLLOWING_TOTAL = "outlinks_following_total";

  // =========================================================================
  // Fetcher Common Crawl extensions
  // =========================================================================

  /** HTTP protocol version group with dynamic counters. */
  public static final String FETCHER_HTTP_PROTOCOL_VERSION_GROUP = "http_protocol_version";

  public static final String FETCHER_HTTP_PROTOCOL_UNKNOWN = "unknown";

  /** SSL/TLS protocol version group with dynamic counters. */
  public static final String FETCHER_TLS_PROTOCOL_VERSION_GROUP = "tls_protocol_version";

  /** IP address version group with two counters: ipv4 and ipv6. */
  public static final String FETCHER_IP_ADDRESS_VERSION_GROUP = "ip_address_version";

  /** Number of fetches over IPv4. */
  public static final String FETCHER_IPV4_TOTAL = "ipv4";

  /** Number of fetches over IPv6. */
  public static final String FETCHER_IPV6_TOTAL = "ipv6";

  /** Archiving of robots.txt captures. */
  public static final String FETCHER_ROBOTSTXT_ARCHIVING_GROUP = "robotstxt_archiving";

  /** Robots.txt not archived: URL rejected by URL filters. */
  public static final String FETCHER_ROBOTSTXT_ARCHIVING_FILTERED_TOTAL = "filtered";

  /** Robots.txt not archived: MIME type rejected. */
  public static final String FETCHER_ROBOTSTXT_ARCHIVING_FILTERED_MIME_TOTAL = "filtered_mime";

  /**
   * Robots.txt not archived: URL path not <code>/robots.txt</code> and
   * disallowed by robots.txt.
   */
  public static final String FETCHER_ROBOTSTXT_ARCHIVING_ROBOTS_DENIED_TOTAL = "robots_denied";

  // =========================================================================
  // Common Crawl's WarcWriter
  // =========================================================================

  /** Counter group for Common Crawl's WARC writer. */
  public static final String GROUP_WARC_WRITER = "warc_writer";

  /** Skipped records because no content (and protocol status) is available. */
  public static final String WARC_WRITER_SKIPPED_NO_CONTENT_TOTAL = "skipped_no_content";

  /** Fixed records: invalid URI normalized. */
  public static final String WARC_WRITER_URI_NORMALIZED_TOTAL = "fixed_uri";

  /** Skipped records because URL is not a valid URI (no WARC-Target-URI). */
  public static final String WARC_WRITER_SKIPPED_INVALID_URI_TOTAL = "skipped_invalid_uri";

  /** Skipped records by content type / MIME type. */
  public static final String WARC_WRITER_SKIPPED_BY_CONTENT_TYPE_TOTAL = "skipped_by_content_type";

  /** Skipped duplicate records. */
  public static final String WARC_WRITER_SKIPPED_DUPLICATE_TOTAL = "skipped_duplicate";

  /** Skipped records: no protocol status. */
  public static final String WARC_WRITER_SKIPPED_NO_PROTOCOL_STATUS_TOTAL = "skipped_no_protocol_status";

  /** Skipped records: unknown protocol status. */
  public static final String WARC_WRITER_SKIPPED_UNKNOWN_PROTOCOL_STATUS_TOTAL = "skipped_unknown_protocol_status";

  /** Prefix for error status of language identification (LID), returned by CLD2 Java bindings. */
  public static final String WARC_WRITER_LID_ERROR_PREFIX = "lid_error: ";

  /** Language identification (LID): no result. */
  public static final String WARC_WRITER_LID_NO_RESULT_TOTAL = "lid_no_result";

  /** Language identification (LID): result is reliable. */
  public static final String WARC_WRITER_LID_RESULT_RELIABLE_TOTAL = "lid_reliable";

  /** Language identification (LID): result is not reliable. */
  public static final String WARC_WRITER_LID_RESULT_NOT_RELIABLE_TOTAL = "lid_not_reliable";

  // =========================================================================
  // Generator Counters
  // =========================================================================

  /** URLs rejected by URL filters. */
  public static final String GENERATOR_URL_FILTERS_REJECTED_TOTAL = "url_filters_rejected_total";

  /** URLs rejected by fetch schedule. */
  public static final String GENERATOR_SCHEDULE_REJECTED_TOTAL = "schedule_rejected_total";

  /** URLs waiting for CrawlDb update. */
  public static final String GENERATOR_WAIT_FOR_UPDATE_TOTAL = "wait_for_update_total";

  /** URLs rejected by JEXL expression. */
  public static final String GENERATOR_EXPR_REJECTED_TOTAL = "expr_rejected_total";

  /** URLs rejected due to status restriction. */
  public static final String GENERATOR_STATUS_REJECTED_TOTAL = "status_rejected_total";

  /** URLs rejected due to score below threshold. */
  public static final String GENERATOR_SCORE_TOO_LOW_TOTAL = "score_too_low_total";

  /** URLs rejected due to fetch interval exceeding threshold. */
  public static final String GENERATOR_INTERVAL_REJECTED_TOTAL = "interval_rejected_total";

  /** URLs skipped due to per-host overflow. */
  public static final String GENERATOR_URLS_SKIPPED_PER_HOST_OVERFLOW_TOTAL = "urls_skipped_per_host_overflow_total";

  /** Hosts affected by per-host overflow. */
  public static final String GENERATOR_HOSTS_AFFECTED_PER_HOST_OVERFLOW_TOTAL = "hosts_affected_per_host_overflow_total";

  // =========================================================================
  // Generator2-specific Counters
  // =========================================================================

  /** Domains affected by per-domain overflow. All remaining URLs of this domain have been skipped, but were not counted. */
  public static final String GENERATOR_DOMAINS_AFFECTED_PER_DOMAIN_OVERFLOW_TOTAL = "domains_affected_per_domain_overflow_total";

  /** Domains affected by max. number of hosts per domain overflow. URLs from further hosts below this domain have been skipped. */
  public static final String GENERATOR_DOMAINS_AFFECTED_PER_MAX_NUM_HOSTS_OVERFLOW_TOTAL = "domains_affected_num_hosts_overflow_total";

  /** URLs skipped due to the max. number of hosts per domain overflow. */
  public static final String GENERATOR_URLS_SKIPPED_PER_MAX_NUM_HOSTS_OVERFLOW_TOTAL = "urls_skipped_per_max_num_host_overflow_total";

  /** URLs skipped due to per-segment overflow. */
  public static final String GENERATOR_URLS_SKIPPED_PER_SEGMENT_OVERFLOW_TOTAL = "urls_skipped_per_segment_overflow_total";

  /**
   * Counter group for items by status, rejected by the fetch schedule. See
   * {@link FetchSchedule#shouldFetch(Text, CrawlDatum, long)}.
   */
  public static final String GROUP_GENERATOR_SCHEDULE_REJECTED_BY_STATUS = "schedule_rejected_by_status";

  /**
   * Counter group for items by status, rejected because the generator score is
   * lower than the minimum score defined per <code>generate.min.score</code>.
   */
  public static final String GROUP_GENERATOR_SCORE_REJECTED_BY_STATUS = "score_rejected_by_status";

  /** Counter group for items by status, selected for fetch. */
  public static final String GROUP_GENERATOR_SELECTED_BY_STATUS = "selected_by_status";

  // =========================================================================
  // Indexer Counters
  // =========================================================================

  /** Documents deleted due to robots noindex. */
  public static final String INDEXER_DELETED_ROBOTS_NOINDEX_TOTAL = "deleted_robots_noindex_total";

  /** Documents deleted because they are gone. */
  public static final String INDEXER_DELETED_GONE_TOTAL = "deleted_gone_total";

  /** Documents deleted due to redirects. */
  public static final String INDEXER_DELETED_REDIRECTS_TOTAL = "deleted_redirects_total";

  /** Documents deleted as duplicates. */
  public static final String INDEXER_DELETED_DUPLICATES_TOTAL = "deleted_duplicates_total";

  /** Documents deleted by indexing filter. */
  public static final String INDEXER_DELETED_BY_INDEXING_FILTER_TOTAL = "deleted_by_indexing_filter_total";

  /** Documents skipped (not modified). */
  public static final String INDEXER_SKIPPED_NOT_MODIFIED_TOTAL = "skipped_not_modified_total";

  /** Documents skipped by indexing filter. */
  public static final String INDEXER_SKIPPED_BY_INDEXING_FILTER_TOTAL = "skipped_by_indexing_filter_total";

  /** Documents indexed (added or updated). */
  public static final String INDEXER_INDEXED_TOTAL = "indexed_total";

  // =========================================================================
  // CrawlDb Counters
  // =========================================================================

  /** URLs filtered during CrawlDb operations. */
  public static final String CRAWLDB_URLS_FILTERED_TOTAL = "urls_filtered_total";

  /** Gone (404) records removed during CrawlDb operations. */
  public static final String CRAWLDB_GONE_RECORDS_REMOVED_TOTAL = "gone_records_removed_total";

  /** Orphan records removed during CrawlDb operations. */
  public static final String CRAWLDB_ORPHAN_RECORDS_REMOVED_TOTAL = "orphan_records_removed_total";

  // =========================================================================
  // Injector Counters
  // =========================================================================

  /** URLs filtered during injection. */
  public static final String INJECTOR_URLS_FILTERED_TOTAL = "urls_filtered_total";

  /** URLs injected. */
  public static final String INJECTOR_URLS_INJECTED_TOTAL = "urls_injected_total";

  /** Unique URLs injected. */
  public static final String INJECTOR_URLS_INJECTED_UNIQUE_TOTAL = "urls_injected_unique_total";

  /** URLs merged with existing CrawlDb entries. */
  public static final String INJECTOR_URLS_MERGED_TOTAL = "urls_merged_total";

  /** URLs purged due to 404 status. */
  public static final String INJECTOR_URLS_PURGED_404_TOTAL = "urls_purged_404_total";

  /** URLs purged by filter. */
  public static final String INJECTOR_URLS_PURGED_FILTER_TOTAL = "urls_purged_filter_total";

  // =========================================================================
  // HostDb Counters
  // =========================================================================

  /** Records filtered in HostDb. */
  public static final String HOSTDB_FILTERED_RECORDS_TOTAL = "filtered_records_total";

  /** Total hosts processed. */
  public static final String HOSTDB_TOTAL_HOSTS_TOTAL = "total_hosts_total";

  /** Hosts skipped (not eligible). */
  public static final String HOSTDB_SKIPPED_NOT_ELIGIBLE_TOTAL = "skipped_not_eligible_total";

  /** Hosts where URL limit was not reached. */
  public static final String HOSTDB_URL_LIMIT_NOT_REACHED_TOTAL = "url_limit_not_reached_total";

  /** New known hosts discovered. */
  public static final String HOSTDB_NEW_KNOWN_HOST_TOTAL = "new_known_host_total";

  /** Rediscovered hosts. */
  public static final String HOSTDB_REDISCOVERED_HOST_TOTAL = "rediscovered_host_total";

  /** Existing known hosts. */
  public static final String HOSTDB_EXISTING_KNOWN_HOST_TOTAL = "existing_known_host_total";

  /** New unknown hosts. */
  public static final String HOSTDB_NEW_UNKNOWN_HOST_TOTAL = "new_unknown_host_total";

  /** Existing unknown hosts. */
  public static final String HOSTDB_EXISTING_UNKNOWN_HOST_TOTAL = "existing_unknown_host_total";

  /** Purged unknown hosts. */
  public static final String HOSTDB_PURGED_UNKNOWN_HOST_TOTAL = "purged_unknown_host_total";

  /** Hosts checked. */
  public static final String HOSTDB_CHECKED_HOSTS_TOTAL = "checked_hosts_total";

  // =========================================================================
  // Deduplication Counters
  // =========================================================================

  /** Documents marked as duplicate. */
  public static final String DEDUP_DOCUMENTS_MARKED_DUPLICATE_TOTAL = "documents_marked_duplicate_total";

  // =========================================================================
  // Redirect Deduplication Counters
  // =========================================================================

  /** Redirects kept as non-duplicates. */
  public static final String DEDUP_REDIRECTS_NOT_DUPLICATES_TOTAL = "redirects_marked_not_duplicate_total";

  /** Redirects in CrawlDb. */
  public static final String DEDUP_REDIRECTS_IN_CRAWLDB_TOTAL = "redirects_in_crawldb_total";

  /** Self-referential redirects in CrawlDb. */
  public static final String DEDUP_REDIRECTS_SELF_REFERENTIAL_TOTAL = "redirects_self_referential_total";

  /** Self-referential redirects kept as non-duplicates. */
  public static final String DEDUP_REDIRECTS_SELF_REFERENTIAL_NOT_DUPLICATES_TOTAL = "redirects_self_referential_marked_not_duplicate_total";

  // =========================================================================
  // Cleaning Job Counters
  // =========================================================================

  /** Documents deleted during cleaning. */
  public static final String CLEANING_DELETED_DOCUMENTS_TOTAL = "deleted_documents_total";

  // =========================================================================
  // WebGraph Counters
  // =========================================================================

  /** Links added to WebGraph. */
  public static final String WEBGRAPH_ADDED_LINKS_TOTAL = "added_links_total";

  /** Links removed from WebGraph. */
  public static final String WEBGRAPH_REMOVED_LINKS_TOTAL = "removed_links_total";

  // =========================================================================
  // Sitemap Counters
  // =========================================================================

  /** Filtered records in sitemap processing. */
  public static final String SITEMAP_FILTERED_RECORDS_TOTAL = "filtered_records_total";

  /** Seeds extracted from sitemaps. */
  public static final String SITEMAP_SEEDS_TOTAL = "sitemap_seeds_total";

  /** Sitemaps discovered from hostname. */
  public static final String SITEMAP_FROM_HOSTNAME_TOTAL = "sitemaps_from_hostname_total";

  /** Sitemaps filtered from hostname. */
  public static final String SITEMAP_FILTERED_FROM_HOSTNAME_TOTAL = "filtered_sitemaps_from_hostname_total";

  /** Failed sitemap fetches. */
  public static final String SITEMAP_FAILED_FETCHES_TOTAL = "failed_fetches_total";

  /** Existing sitemap entries. */
  public static final String SITEMAP_EXISTING_ENTRIES_TOTAL = "existing_sitemap_entries_total";

  /** New sitemap entries. */
  public static final String SITEMAP_NEW_ENTRIES_TOTAL = "new_sitemap_entries_total";

  // =========================================================================
  // SitemapInjector Counters
  // =========================================================================

  /** SitemapInjector counter group. */
  public static final String GROUP_SITEMAP_INJECTOR = "sitemap_injector";

  /** Failed to fetch sitemap content, disallowed per robots.txt. */
  public static final String SITEMAP_ROBOTSTXT_DISALLOW_TOTAL = "sitemap_robotstxt_disallow";

  /** Sitemap failed to parse. */
  public static final String SITEMAP_FAILED_TO_PARSE_TOTAL = "sitemaps_failed_to_parse";

  /** Prefix for sitemap type counter. */
  public static final String SITEMAP_TYPE_PREFIX = "sitemap_type_";

  /** Sitemaps processed total. */
  public static final String SITEMAP_PROCESSED_TOTAL = "sitemaps_processed";

  /** Sitemap index: affected by URL limit. */
  public static final String SITEMAP_INDEX_AFFECTED_BY_URL_LIMIT_TOTAL = "sitemap_index_url_limit";

  /** Sitemap index: affected by depth limit. */
  public static final String SITEMAP_INDEX_AFFECTED_BY_DEPTH_LIMIT_TOTAL = "sitemap_index_depth_limit";

  /** Sitemap index: affected by time limit. */
  public static final String SITEMAP_INDEX_AFFECTED_BY_TIME_LIMIT_TOTAL = "sitemap_index_time_limit";

  /** Sitemap index: skipped because no URLs found after 50% of time limit. */
  public static final String SITEMAP_INDEX_NO_URLS_AFTER_50_PERCENT_OF_TIME_LIMIT_TOTAL = "sitemap_index_no_urls_after_50_percent_of_time_limit";

  /** Sitemap index: skipped because of too many fetch failures. */
  public static final String SITEMAP_INDEX_TOO_MANY_FAILURES_TOTAL = "sitemap_index_too_many_failures";

  /** Sitemap index: processed sitemaps. */
  public static final String SITEMAP_INDEX_PROCESSED_SITEMAPS_TOTAL = "sitemap_index_processed_sitemaps";

  /** Skipped duplicated or recursive sitemap URLs. */
  public static final String SITEMAP_SKIPPED_DUPLICATE_OR_RECURSIVE_URL_TOTAL = "sitemap_skipped_duplicate_or_recursive_sitemap_url";

  /** Sitemap index: affected by max. number of sitemaps in index. */
  public static final String SITEMAP_INDEX_MAX_SITEMAPS_LIMIT_TOTAL = "sitemap_index_max_sitemaps_limit";

  /** Sitemap failed to fetch. */
  public static final String SITEMAP_FAILED_TO_FETCH_TOTAL = "sitemap_failed_to_fetch";

  /** Sitemap skipped because of overlong URL. */
  public static final String SITEMAP_SKIPPED_OVERLONG_URL_TOTAL = "sitemap_skipped_overlong_url";

  /** Sitemap rejected by URL filters */
  public static final String SITEMAP_REJECTED_BY_URL_FILTERS_TOTAL = "sitemap_rejected_by_url_filters";

  /** Sitemap skipped, too many failures per host. */
  public static final String SITEMAP_SKIPPED_TOO_MANY_FAILURES_PER_HOST_TOTAL = "sitemap_skipped_too_many_failures_per_host";

  /** Could not fetch sitemap content, protocol not supported. */
  public static final String SITEMAP_PROTOCOL_NOT_SUPPORTED_TOTAL = "sitemap_protocol_not_supported";

  /** Failed to fetch sitemap content because of timeout. */
  public static final String SITEMAP_FAILED_TO_FETCH_TIMEOUT_TOTAL = "sitemap_failed_to_fetch_timeout";

  /** Failed to fetch sitemap content because of exception. */
  public static final String SITEMAP_FAILED_TO_FETCH_EXCEPTION_TOTAL = "sitemap_failed_to_fetch_exception";

  /** Sitemap redirect. */
  public static final String SITEMAP_REDIRECT_TOTAL = "sitemap_redirect";

  /** Sitemap redirect target rejected by URL filters */
  public static final String SITEMAP_REDIRECT_TARGET_REJECTED_BY_URL_FILTERS_TOTAL = "sitemap_redirect_target_rejected_by_url_filters";

  /** Sitemap redirect limit exceeded (max. number of redirects followed). */
  public static final String SITEMAP_REDIRECT_LIMIT_EXCEEDED_TOTAL = "sitemap_redirect_limit_exceeded";

  /** Failed to fetch sitemap content, HTTP status != 200. */
  public static final String SITEMAP_FAILED_TO_FETCH_CONTENT_HTTP_STATUS_CODE_NOT_200_TOTAL = "sitemap_failed_to_fetch_http_status_code_not_200";

  /** Failed to fetch sitemap content, empty content. */
  public static final String SITEMAP_EMPTY_CONTENT_TOTAL = "sitemap_empty_content";

  /** Empty sitemap. */
  public static final String SITEMAP_EMPTY_TOTAL = "sitemap_empty";

  /** Sitemap URL limit reached. */
  public static final String SITEMAP_URL_LIMIT_REACHED_TOTAL = "sitemap_url_limit_reached";

  /** URLs randomly skipped. */
  public static final String SITEMAP_RANDOM_SKIP_TOTAL = "urls_random_skip";

  /** URLs from sitemaps rejected, host limit reached. */
  public static final String SITEMAP_URLS_SKIPPED_HOST_LIMIT_REACHED_TOTAL = "urls_skipped_host_limit_reached";

  /** URLs from sitemaps rejected, target not allowed by cross-submit. */
  public static final String SITEMAP_URLS_SKIPPED_NOT_ALLOWED_BY_CROSS_SUBMITS_TOTAL = "urls_skipped_not_allowed_by_cross_submits";

  /** URLs from sitemaps rejected by URL filters. */
  public static final String SITEMAP_URLS_FROM_REJECTED_BY_URL_FILTERS = "urls_from_sitemaps_rejected_by_url_filters";

  /** URLs from sitemaps injected. */
  public static final String SITEMAP_URLS_INJECTED = "urls_from_sitemaps_injected";

  // =========================================================================
  // WARC Exporter Counters
  // =========================================================================

  /** Missing content in WARC export. */
  public static final String WARC_MISSING_CONTENT_TOTAL = "missing_content_total";

  /** Missing metadata in WARC export. */
  public static final String WARC_MISSING_METADATA_TOTAL = "missing_metadata_total";

  /** Omitted empty responses in WARC export. */
  public static final String WARC_OMITTED_EMPTY_RESPONSE_TOTAL = "omitted_empty_response_total";

  /** WARC records generated. */
  public static final String WARC_RECORDS_GENERATED_TOTAL = "records_generated_total";

  // =========================================================================
  // Domain Statistics Counters (enum-based, kept for compatibility)
  // =========================================================================

  /** Fetched URLs in domain statistics. */
  public static final String DOMAIN_STATS_FETCHED_TOTAL = "fetched_total";

  /** Not fetched URLs in domain statistics. */
  public static final String DOMAIN_STATS_NOT_FETCHED_TOTAL = "not_fetched_total";

  /** Empty results in domain statistics. */
  public static final String DOMAIN_STATS_EMPTY_RESULT_TOTAL = "empty_result_total";

  // =========================================================================
  // UrlCleaner
  // =========================================================================

  public static final String GROUP_URLCLEANER = "urlcleaner";

  public static final String URLCLEANER_REJECTED_TOTAL = "urls_rejected";

  public static final String URLCLEANER_REJECTED_INVALID_DOMAIN_TOTAL = "urls_rejected_invalid_domain";

  public static final String URLCLEANER_ACCEPTED_UNCHANGED_TOTAL = "urls_accepted_unchanged";

  public static final String URLCLEANER_ACCEPTED_NORMALIZED_TOTAL = "urls_accepted_normalized";

  // =========================================================================
  // UrlSampler and UrlSamplerHost
  // =========================================================================

  public static final String GROUP_URLSAMPLER = "urlsampler";

  public static final String GROUP_URLSAMPLER_HOST = "urlsamplerhost";

  public static final String URLSAMPLER_MALFORMED_URL_TOTAL = "malformed_url";

  public static final String URLSAMPLER_SKIPPED_MAX_URLS_TOTAL = "skipped_max_urls";

  public static final String URLSAMPLER_SKIPPED_MAX_URLS_PER_HOST_TOTAL = "skipped_max_urls_per_host";

  public static final String URLSAMPLER_SKIPPED_MAX_HOSTS_TOTAL = "skipped_max_hosts";

  public static final String URLSAMPLER_HOSTS = "hosts";

  public static final String URLSAMPLER_URLS = "urls";

  public static final String URLSAMPLER_HOSTS_WITH_LIMIT = "hosts_with_limit";

  public static final String URLSAMPLER_URLS_HOST_WITH_LIMIT = "urls_host_with_limit";

  public static final String URLSAMPLER_HOSTS_WITHOUT_LIMIT = "hosts_without_limit";

  public static final String URLSAMPLER_URLS_HOST_WITHOUT_LIMIT = "urls_host_without_limit";

  public static final String URLSAMPLER_URLS_SAMPLED = "urls_sampled";

  public static final String URLSAMPLER_HOSTS_SAMPLED = "hosts_sampled";

  public static final String URLSAMPLER_HOSTS_WITH_LIMIT_SAMPLED = "hosts_with_limit_sampled";

  public static final String URLSAMPLER_URLS_HOST_WITH_LIMIT_SAMPLED = "urls_host_with_limit_sampled";

  public static final String URLSAMPLER_HOSTS_WITHOUT_LIMIT_SAMPLED = "hosts_without_limit_sampled";

  public static final String URLSAMPLER_URLS_HOST_WITHOUT_LIMIT_SAMPLED = "urls_host_without_limit_sampled";

  public static final String URLSAMPLER_SKIPPED_MAX_URLS_PER_HOST = "skipped_max_urls_per_host";

  public static final String URLSAMPLER_SKIPPED_RANDOM = "skipped_random";

  // =========================================================================
  // Latency Metric Prefixes (used with LatencyTracker)
  // =========================================================================

  /**
   * Prefix for fetch latency metrics.
   * Used with {@link LatencyTracker} to emit fetch timing counters.
   */
  public static final String FETCHER_LATENCY = "fetch_latency";

  /**
   * Prefix for parse latency metrics.
   * Used with {@link LatencyTracker} to emit parse timing counters.
   */
  public static final String PARSER_LATENCY = "parse_latency";

  /**
   * Prefix for indexer latency metrics.
   * Used with {@link LatencyTracker} to emit indexing timing counters.
   */
  public static final String INDEXER_LATENCY = "index_latency";

  // =========================================================================
  // Common Error Counter Names (used with component-specific groups)
  // These constants are shared across all components for consistent error
  // categorization. Use with ErrorTracker for automatic classification.
  // =========================================================================

  /**
   * Total errors across all categories.
   * This is incremented alongside any category-specific error counter.
   */
  public static final String ERROR_TOTAL = "errors_total";

  /**
   * Network-related errors.
   * Includes: IOException, SocketException, ConnectException, UnknownHostException
   */
  public static final String ERROR_NETWORK_TOTAL = "errors_network_total";

  /**
   * Protocol errors.
   * Includes: ProtocolException, ProtocolNotFound
   */
  public static final String ERROR_PROTOCOL_TOTAL = "errors_protocol_total";

  /**
   * Parsing errors.
   * Includes: ParseException, ParserNotFound
   */
  public static final String ERROR_PARSING_TOTAL = "errors_parsing_total";

  /**
   * URL-related errors.
   * Includes: MalformedURLException, URLFilterException
   */
  public static final String ERROR_URL_TOTAL = "errors_url_total";

  /**
   * Scoring filter errors.
   * Includes: ScoringFilterException
   */
  public static final String ERROR_SCORING_TOTAL = "errors_scoring_total";

  /**
   * Indexing filter errors.
   * Includes: IndexingException
   */
  public static final String ERROR_INDEXING_TOTAL = "errors_indexing_total";

  /**
   * Timeout errors.
   * Includes: SocketTimeoutException, connection timeouts
   */
  public static final String ERROR_TIMEOUT_TOTAL = "errors_timeout_total";

  /**
   * Other uncategorized errors.
   * Used as fallback for exceptions not matching any specific category.
   */
  public static final String ERROR_OTHER_TOTAL = "errors_other_total";
}

