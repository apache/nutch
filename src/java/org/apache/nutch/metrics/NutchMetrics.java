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

  /** Counter group for Common Crawl data dumper tool. */
  public static final String GROUP_COMMONCRAWL_DUMPER = "nutch_commoncrawl_dumper";

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

