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
package org.apache.nutch.anthelion.any23;

/**
 * THIS IS NOT USED
 * 
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class AnthelionParser {
  // implements Parser {
}
// public static final Logger LOG =
// LoggerFactory.getLogger("org.apache.nutch.parse.html");
//
// // I used 1000 bytes at first, but found that some documents have
// // meta tag well past the first 1000 bytes.
// // (e.g. http://cn.promo.yahoo.com/customcare/music.html)
// private static final int CHUNK_SIZE = 2000;
//
// public static final String META_CONTAINS_SEM = "containsSem";
//
// public static final String META_CONTAINS_SEM_FATHER = "containsSemFather";
//
// public static final String META_CONTAINS_SEM_FATHER_FOR_SUB =
// "containsSemFatherforSub";
//
// // NUTCH-1006 Meta equiv with single quotes not accepted
// private static Pattern metaPattern =
// Pattern.compile("<meta\\s+([^>]*http-equiv=(\"|')?content-type(\"|')?[^>]*)>",
// Pattern.CASE_INSENSITIVE);
// private static Pattern charsetPattern =
// Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
// Pattern.CASE_INSENSITIVE);
//
// private String parserImpl;
//
// /**
// * Given a <code>byte[]</code> representing an html file of an
// * <em>unknown</em> encoding, read out 'charset' parameter in the meta tag
// * from the first <code>CHUNK_SIZE</code> bytes. If there's no meta tag for
// * Content-Type or no charset is specified, <code>null</code> is returned.
// * <br />
// * FIXME: non-byte oriented character encodings (UTF-16, UTF-32) can't be
// * handled with this. We need to do something similar to what's done by
// * mozilla
// * (http://lxr.mozilla.org/seamonkey/source/parser/htmlparser/src/nsParser.
// * cpp#1993). See also http://www.w3.org/TR/REC-xml/#sec-guessing <br />
// *
// * @param content
// * <code>byte[]</code> representation of an html file
// */
//
// private static String sniffCharacterEncoding(byte[] content) {
// int length = content.length < CHUNK_SIZE ? content.length : CHUNK_SIZE;
//
// // We don't care about non-ASCII parts so that it's sufficient
// // to just inflate each byte to a 16-bit value by padding.
// // For instance, the sequence {0x41, 0x82, 0xb7} will be turned into
// // {U+0041, U+0082, U+00B7}.
// String str = "";
// try {
// str = new String(content, 0, length, Charset.forName("ASCII").toString());
// } catch (UnsupportedEncodingException e) {
// // code should never come here, but just in case...
// return null;
// }
//
// Matcher metaMatcher = metaPattern.matcher(str);
// String encoding = null;
// if (metaMatcher.find()) {
// Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
// if (charsetMatcher.find())
// encoding = new String(charsetMatcher.group(1));
// }
//
// return encoding;
// }
//
// private String defaultCharEncoding;
//
// private Configuration conf;
//
// private DOMContentUtils utils;
//
// private HtmlParseFilters htmlParseFilters;
//
// private String cachingPolicy;
//
// /**
// * The evil name spaces
// */
// private List<String> evilNamespaces;
// /**
// * the no evil name spaces
// */
// private List<String> notSoEvilNamespaces;
// /**
// * The extrac parameters
// */
// private static ExtractionParameters any23Params;
//
// static {
// any23Params = ExtractionParameters.newDefault();
// any23Params.setFlag("any23.extraction.metadata.timesize", false);
// any23Params.setFlag("any23.extraction.head.meta", false);
// }
//
// /**
// * The Any23 Extractor
// */
// private Any23 any23;
//
// public ParseResult getParse(Content content) {
//
// // Generate source document
// byte[] byteContent = content.getContent();
// String contentType = content.getContentType();
// String url = content.getUrl();
// ByteArrayDocumentSource source = new ByteArrayDocumentSource(byteContent,
// "http://www.test.de",
// "application/xhtml+xml;q=0.1");
//
// // StringBuffer writer = new StringBuffer();
//
// // gets outlink. we inject here code of parse-html
// HTMLMetaTags metaTags = new HTMLMetaTags();
//
// URL base;
// try {
// base = new URL(content.getBaseUrl());
// } catch (MalformedURLException e) {
// return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
// }
//
// String text = "";
// boolean containsSem = false;
// String title = "";
// Outlink[] outlinks = new Outlink[0];
// Metadata metadata = new Metadata();
//
// // parse the content
// DocumentFragment root;
// try {
// byte[] contentInOctets = content.getContent();
// InputSource input = new InputSource(new
// ByteArrayInputStream(contentInOctets));
//
// EncodingDetector detector = new EncodingDetector(conf);
// detector.autoDetectClues(content, true);
// detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
// String encoding = detector.guessEncoding(content, defaultCharEncoding);
//
// metadata.set(Metadata.ORIGINAL_CHAR_ENCODING, encoding);
// metadata.set(Metadata.CHAR_ENCODING_FOR_CONVERSION, encoding);
//
// input.setEncoding(encoding);
// if (LOG.isTraceEnabled()) {
// LOG.trace("Parsing...");
// }
// root = parse(input);
// } catch (IOException e) {
// return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
// } catch (DOMException e) {
// return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
// } catch (SAXException e) {
// return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
// } catch (Exception e) {
// LOG.error("Error: ", e);
// return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
// }
//
// try {
//
// text = Any23Extractor.getTriplesFromContent(source);
// if (text != null) {
// containsSem = true;
//
// }
// // FilterableTripleHandler triple = new
// // FilterableTripleHandler(writer, evilNamespaces,
// // notSoEvilNamespaces);
// // //extract
// // any23.extract(any23Params, source, triple);
// } catch (Exception e) {
// e.printStackTrace();
// }
//
// // get meta directives
// HTMLMetaProcessor.getMetaTags(metaTags, root, base);
// if (LOG.isTraceEnabled()) {
// LOG.trace("Meta tags for " + base + ": " + metaTags.toString());
// }
//
// // check meta directives
// if (!metaTags.getNoIndex()) { // okay to index
// StringBuffer sb = new StringBuffer();
// if (LOG.isTraceEnabled()) {
// LOG.trace("Getting text...");
// }
// // utils.getText(sb, root); // extract text
// // text = writer.toString();
// sb.setLength(0);
// if (LOG.isTraceEnabled()) {
// LOG.trace("Getting title...");
// }
// utils.getTitle(sb, root); // extract title
// title = sb.toString().trim();
// }
//
// // writer = null;
//
// if (!metaTags.getNoFollow()) { // okay to follow links
// ArrayList<Outlink> l = new ArrayList<Outlink>(); // extract outlinks
// URL baseTag = utils.getBase(root);
// if (LOG.isTraceEnabled()) {
// LOG.trace("Getting links...");
// }
// utils.getOutlinks(baseTag != null ? baseTag : base, l, root);
// outlinks = l.toArray(new Outlink[l.size()]);
// if (LOG.isTraceEnabled()) {
// LOG.trace("found " + outlinks.length + " outlinks in " + content.getUrl());
// }
// }
//
// ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
// if (metaTags.getRefresh()) {
// status.setMinorCode(ParseStatus.SUCCESS_REDIRECT);
// status.setArgs(
// new String[] { metaTags.getRefreshHref().toString(),
// Integer.toString(metaTags.getRefreshTime()) });
// }
// ParseData parseData = new ParseData(status, title, outlinks,
// content.getMetadata(), metadata);
// ParseResult parseResult = ParseResult.createParseResult(content.getUrl(), new
// ParseImpl(text, parseData));
//
// Parse parse = parseResult.get(content.getUrl());
// parse.getData().getContentMeta().set(META_CONTAINS_SEM,
// Boolean.toString(containsSem));
//
// // run filters on parse
// // ParseResult filteredParse = this.htmlParseFilters.filter(content,
// // parseResult, metaTags, root);
// if (metaTags.getNoCache()) { // not okay to cache
// for (Map.Entry<org.apache.hadoop.io.Text, Parse> entry : parseResult)
// entry.getValue().getData().getParseMeta().set(Nutch.CACHING_FORBIDDEN_KEY,
// cachingPolicy);
// }
// return parseResult;
// }
//
// private DocumentFragment parse(InputSource input) throws Exception {
// if (parserImpl.equalsIgnoreCase("tagsoup"))
// return parseTagSoup(input);
// else
// return parseNeko(input);
// }
//
// private DocumentFragment parseTagSoup(InputSource input) throws Exception {
// HTMLDocumentImpl doc = new HTMLDocumentImpl();
// DocumentFragment frag = doc.createDocumentFragment();
// DOMBuilder builder = new DOMBuilder(doc, frag);
// org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
// reader.setContentHandler(builder);
// reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
// reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
// reader.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
// reader.parse(input);
// return frag;
// }
//
// private DocumentFragment parseNeko(InputSource input) throws Exception {
// DOMFragmentParser parser = new DOMFragmentParser();
// try {
// parser.setFeature("http://cyberneko.org/html/features/augmentations", true);
// parser.setProperty("http://cyberneko.org/html/properties/default-encoding",
// defaultCharEncoding);
// parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset",
// true);
// parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
// false);
// parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment",
// true);
// parser.setFeature("http://cyberneko.org/html/features/report-errors",
// LOG.isTraceEnabled());
// } catch (SAXException e) {
// }
// // convert Document to DocumentFragment
// HTMLDocumentImpl doc = new HTMLDocumentImpl();
// doc.setErrorChecking(false);
// DocumentFragment res = doc.createDocumentFragment();
// DocumentFragment frag = doc.createDocumentFragment();
// parser.parse(input, frag);
// res.appendChild(frag);
//
// try {
// while (true) {
// frag = doc.createDocumentFragment();
// parser.parse(input, frag);
// if (!frag.hasChildNodes())
// break;
// if (LOG.isInfoEnabled()) {
// LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
// }
// res.appendChild(frag);
// }
// } catch (Exception e) {
// LOG.error("Error: ", e);
// }
// ;
// return res;
// }
//
// public static void main(String[] args) throws Exception {
// // LOG.setLevel(Level.FINE);
// String name = args[0];
// String url = "file:" + name;
// File file = new File(name);
// byte[] bytes = new byte[(int) file.length()];
// DataInputStream in = new DataInputStream(new FileInputStream(file));
// in.readFully(bytes);
// Configuration conf = NutchConfiguration.create();
// AnthelionParser parser = new AnthelionParser();
// parser.setConf(conf);
// Parse parse = parser.getParse(new Content(url, url, bytes, "text/html", new
// Metadata(), conf)).get(url);
// System.out.println("data: " + parse.getData());
//
// System.out.println("text: " + parse.getText());
//
// String contains = parse.getData().getMeta(META_CONTAINS_SEM);
// System.out.println("contains: " + contains);
//
// }
//
// /**
// * loading configuration
// */
// public void setConf(Configuration conf) {
//
// this.conf = conf;
//
// // generate Any23
// // List<ExtractorFactory<?>> factories = new
// // ArrayList<ExtractorFactory<?>>();
// // if (getConf().getBoolean("wdc.extractors.contain.RDFaExtractor",
// // true)) {
// // factories.add(RDFaExtractor.factory);
// // }
// // if (getConf().getBoolean("wdc.extractors.contain.RDFa11Extractor",
// // true)) {
// // factories.add(RDFa11Extractor.factory);
// // }
// // if (getConf().getBoolean("wdc.extractors.contain.MicrodataExtractor",
// // false)) {
// // factories.add(MicrodataExtractor.);
// // }
// // if (getConf().getBoolean("wdc.extractors.contain.GeoExtractor",
// // false)) {
// // factories.add(GeoExtractor.factory);
// // }
// this.any23 = new Any23();
//
// // name spaces
// final String[] evilNs = { "http://vocab.sindice.net/any23#",
// "http://www.w3.org/1999/xhtml/vocab#" };
// final String[] noEvilNs = { "http://vocab.sindice.net/any23#hrecipe/" };
// this.evilNamespaces =
// Arrays.asList(getConf().getStrings("wdc.evilNamespaces", evilNs));
// this.notSoEvilNamespaces =
// Arrays.asList(getConf().getStrings("wdc.noEvilNamespaces", noEvilNs));
//
// // inject parse-html plugin
// this.htmlParseFilters = new HtmlParseFilters(getConf());
// this.parserImpl = getConf().get("parser.html.impl", "neko");
// this.defaultCharEncoding = getConf().get("parser.character.encoding.default",
// "windows-1252");
// this.utils = new DOMContentUtils(conf);
// this.cachingPolicy = getConf().get("parser.caching.forbidden.policy",
// Nutch.CACHING_FORBIDDEN_CONTENT);
// }
//
// public Configuration getConf() {
// return this.conf;
// }
// }
