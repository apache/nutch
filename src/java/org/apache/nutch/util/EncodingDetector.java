package org.apache.nutch.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

/**
 * A simple class for detecting character encodings.
 *
 * <p>
 * Broadly this encompasses two functions, which are distinctly separate:
 *
 * <ol>
 *  <li>Auto detecting a set of "clues" from input text.</li>
 *  <li>Taking a set of clues and making a "best guess" as to the
 *      "real" encoding.</li>
 * </ol>
 * </p>
 *
 * <p>
 * A caller will often have some extra information about what the
 * encoding might be (e.g. from the HTTP header or HTML meta-tags, often
 * wrong but still potentially useful clues). The types of clues may differ
 * from caller to caller. Thus a typical calling sequence is:
 * <ul>
 *    <li>Run step (1) to generate a set of auto-detected clues;</li>
 *    <li>Combine these clues with the caller-dependent "extra clues"
 *        available;</li>
 *    <li>Run step (2) to guess what the most probable answer is.</li>
 * </p>
 */
public class EncodingDetector {

  private class EncodingClue {
    private String value;
    private String source;
    private int confidence;

    // Constructor for clues with no confidence values (ignore thresholds)
    public EncodingClue(String value, String source) {
      this(value, source, NO_THRESHOLD);
    }

    public EncodingClue(String value, String source, int confidence) {
      this.value = value.toLowerCase();
      this.source = source;
      this.confidence = confidence;
    }

    public String getSource() {
      return source;
    }

    public String getValue() {
      return value;
    }

    public String toString() {
      return value + " (" + source +
           ((confidence >= 0) ? ", " + confidence + "% confidence" : "") + ")";
    }

    public boolean isEmpty() {
      return (value==null || "".equals(value));
    }

    public boolean meetsThreshold() {
      return (confidence < 0 ||
               (minConfidence >= 0 && confidence >= minConfidence));
    }
  }

  public static final Log LOG = LogFactory.getLog(EncodingDetector.class);

  public static final int NO_THRESHOLD = -1;

  public static final String MIN_CONFIDENCE_KEY =
    "encodingdetector.charset.min.confidence";

  private static final HashMap<String, String> ALIASES =
    new HashMap<String, String>();

  private static final HashSet<String> DETECTABLES = new HashSet<String>();

  // CharsetDetector will die without a minimum amount of data.
  private static final int MIN_LENGTH=4;

  static {
    DETECTABLES.add("text/html");
    DETECTABLES.add("text/plain");
    DETECTABLES.add("text/richtext");
    DETECTABLES.add("text/rtf");
    DETECTABLES.add("text/sgml");
    DETECTABLES.add("text/tab-separated-values");
    DETECTABLES.add("text/xml");
    DETECTABLES.add("application/rss+xml");
    DETECTABLES.add("application/xhtml+xml");
    /*
     * the following map is not an alias mapping table, but
     * maps character encodings which are often used in mislabelled
     * documents to their correct encodings. For instance,
     * there are a lot of documents labelled 'ISO-8859-1' which contain
     * characters not covered by ISO-8859-1 but covered by windows-1252.
     * Because windows-1252 is a superset of ISO-8859-1 (sharing code points
     * for the common part), it's better to treat ISO-8859-1 as
     * synonymous with windows-1252 than to reject, as invalid, documents
     * labelled as ISO-8859-1 that have characters outside ISO-8859-1.
     */
    ALIASES.put("ISO-8859-1", "windows-1252");
    ALIASES.put("EUC-KR", "x-windows-949");
    ALIASES.put("x-EUC-CN", "GB18030");
    ALIASES.put("GBK", "GB18030");
    //ALIASES.put("Big5", "Big5HKSCS");
    //ALIASES.put("TIS620", "Cp874");
    //ALIASES.put("ISO-8859-11", "Cp874");

  }

  private int minConfidence;

  private CharsetDetector detector;

  private List<EncodingClue> clues;

  public EncodingDetector(Configuration conf) {
    minConfidence = conf.getInt(MIN_CONFIDENCE_KEY, -1);
    detector = new CharsetDetector();
    clues = new ArrayList<EncodingClue>();
  }

  public void autoDetectClues(Content content, boolean filter) {
    byte[] data = content.getContent();

    if (minConfidence >= 0 && DETECTABLES.contains(content.getContentType())
        && data.length > MIN_LENGTH) {
      CharsetMatch[] matches = null;

      // do all these in a try/catch; setText and detect/detectAll
      // will sometimes throw exceptions
      try {
        detector.enableInputFilter(filter);
        if (data.length > MIN_LENGTH) {
          detector.setText(data);
          matches = detector.detectAll();
        }
      } catch (Exception e) {
        LOG.debug("Exception from ICU4J (ignoring): ");
        e.printStackTrace(LogUtil.getDebugStream(LOG));
      }

      if (matches != null) {
        for (CharsetMatch match : matches) {
          addClue(match.getName(), "detect", match.getConfidence());
        }
      }
    }

    // add character encoding coming from HTTP response header
    addClue(parseCharacterEncoding(
        content.getMetadata().get(Response.CONTENT_TYPE)), "header");
  }

  public void addClue(String value, String source, int confidence) {
    if (value == null || "".equals(value)) {
      return;
    }
    value = resolveEncodingAlias(value);
    if (value != null) {
      clues.add(new EncodingClue(value, source, confidence));
    }
  }

  public void addClue(String value, String source) {
    addClue(value, source, NO_THRESHOLD);
  }

  /**
   * Guess the encoding with the previously specified list of clues.
   *
   * @param content Content instance
   * @param defaultValue Default encoding to return if no encoding can be
   * detected with enough confidence. Note that this will <b>not</b> be
   * normalized with {@link EncodingDetector#resolveEncodingAlias}
   *
   * @return Guessed encoding or defaultValue
   */
  public String guessEncoding(Content content, String defaultValue) {
    /*
     * This algorithm could be replaced by something more sophisticated;
     * ideally we would gather a bunch of data on where various clues
     * (autodetect, HTTP headers, HTML meta tags, etc.) disagree, tag each with
     * the correct answer, and use machine learning/some statistical method
     * to generate a better heuristic.
     */

    String base = content.getBaseUrl();

    if (LOG.isTraceEnabled()) {
      findDisagreements(base, clues);
    }

    /*
     * Go down the list of encoding "clues". Use a clue if:
     *  1. Has a confidence value which meets our confidence threshold, OR
     *  2. Doesn't meet the threshold, but is the best try,
     *     since nothing else is available.
     */
    EncodingClue defaultClue = new EncodingClue(defaultValue, "default");
    EncodingClue bestClue = defaultClue;

    for (EncodingClue clue : clues) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(base + ": charset " + clue);
      }
      String charset = clue.value;
      if (minConfidence >= 0 && clue.confidence >= minConfidence) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(base + ": Choosing encoding: " + charset +
                    " with confidence " + clue.confidence);
        }
        return resolveEncodingAlias(charset).toLowerCase();
      } else if (clue.confidence == NO_THRESHOLD && bestClue == defaultClue) {
        bestClue = clue;
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(base + ": Choosing encoding: " + bestClue);
    }
    return bestClue.value.toLowerCase();
  }

  /** Clears all clues. */
  public void clearClues() {
    clues.clear();
  }

  /*
   * Strictly for analysis, look for "disagreements." The top guess from
   * each source is examined; if these meet the threshold and disagree, then
   * we log the information -- useful for testing or generating training data
   * for a better heuristic.
   */
  private void findDisagreements(String url, List<EncodingClue> newClues) {
    HashSet<String> valsSeen = new HashSet<String>();
    HashSet<String> sourcesSeen = new HashSet<String>();
    boolean disagreement = false;
    for (int i = 0; i < newClues.size(); i++) {
      EncodingClue clue = newClues.get(i);
      if (!clue.isEmpty() && !sourcesSeen.contains(clue.source)) {
        if (valsSeen.size() > 0 && !valsSeen.contains(clue.value)
            && clue.meetsThreshold()) {
          disagreement = true;
        }
        if (clue.meetsThreshold()) {
          valsSeen.add(clue.value);
        }
        sourcesSeen.add(clue.source);
      }
    }
    if (disagreement) {
      // dump all values in case of disagreement
      StringBuffer sb = new StringBuffer();
      sb.append("Disagreement: "+url+"; ");
      for (int i = 0; i < newClues.size(); i++) {
        if (i>0) {
          sb.append(", ");
        }
        sb.append(newClues.get(i));
      }
      LOG.trace(sb.toString());
    }
  }

  public static String resolveEncodingAlias(String encoding) {
    if (encoding == null || !Charset.isSupported(encoding))
      return null;
    String canonicalName = new String(Charset.forName(encoding).name());
    return ALIASES.containsKey(canonicalName) ? ALIASES.get(canonicalName)
                                              : canonicalName;
  }

  /**
   * Parse the character encoding from the specified content type header.
   * If the content type is null, or there is no explicit character encoding,
   * <code>null</code> is returned.
   * <br />
   * This method was copied from org.apache.catalina.util.RequestUtil,
   * which is licensed under the Apache License, Version 2.0 (the "License").
   *
   * @param contentType a content type header
   */
  public static String parseCharacterEncoding(String contentType) {
    if (contentType == null)
      return (null);
    int start = contentType.indexOf("charset=");
    if (start < 0)
      return (null);
    String encoding = contentType.substring(start + 8);
    int end = encoding.indexOf(';');
    if (end >= 0)
      encoding = encoding.substring(0, end);
    encoding = encoding.trim();
    if ((encoding.length() > 2) && (encoding.startsWith("\""))
      && (encoding.endsWith("\"")))
      encoding = encoding.substring(1, encoding.length() - 1);
    return (encoding.trim());

  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: EncodingDetector <file>");
      System.exit(1);
    }

    Configuration conf = NutchConfiguration.create();
    EncodingDetector detector =
      new EncodingDetector(NutchConfiguration.create());

    // do everything as bytes; don't want any conversion
    BufferedInputStream istr =
      new BufferedInputStream(new FileInputStream(args[0]));
    ByteArrayOutputStream ostr = new ByteArrayOutputStream();
    byte[] bytes = new byte[1000];
    boolean more = true;
    while (more) {
      int len = istr.read(bytes);
      if (len < bytes.length) {
        more = false;
        if (len > 0) {
          ostr.write(bytes, 0, len);
        }
      } else {
        ostr.write(bytes);
      }
    }

    byte[] data = ostr.toByteArray();

    // make a fake Content
    Content content =
      new Content("", "", data, "text/html", new Metadata(), conf);

    detector.autoDetectClues(content, true);
    String encoding = detector.guessEncoding(content,
        conf.get("parser.character.encoding.default"));
    System.out.println("Guessed encoding: " + encoding);
  }

}
