package org.apache.nutch.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.StringUtil;
import org.archive.format.http.HttpHeaders;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.uid.UUIDGenerator;
import org.archive.util.DateUtils;
import org.archive.util.anvl.ANVLRecord;

public class WARCUtils {
    public final static String SOFTWARE = "software";
    public final static String HTTP_HEADER_FROM = "http-header-from";
    public final static String HTTP_HEADER_USER_AGENT = "http-header-user-agent";
    public final static String HOSTNAME = "hostname";
    public final static String ROBOTS = "robots";
    public final static String OPERATOR = "operator";
    public final static String FORMAT = "format";
    public final static String CONFORMS_TO = "conformsTo";
    public final static String IP = "ip";
    public final static UUIDGenerator generator = new UUIDGenerator();

    public static final ANVLRecord getWARCInfoContent(Configuration conf) {
        ANVLRecord record = new ANVLRecord();

        // informative headers
        record.addLabelValue(FORMAT, "WARC File Format 1.0");
        record.addLabelValue(CONFORMS_TO, "http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf");

        record.addLabelValue(SOFTWARE, conf.get("http.agent.name", ""));
        record.addLabelValue(HTTP_HEADER_USER_AGENT,
                getAgentString(conf.get("http.agent.name", ""),
                        conf.get("http.agent.version", ""),
                        conf.get("http.agent.description", ""),
                        conf.get("http.agent.url", ""),
                        conf.get("http.agent.email", "")));
        record.addLabelValue(HTTP_HEADER_FROM,
                conf.get("http.agent.email", ""));

        try {
            record.addLabelValue(HOSTNAME, getHostname(conf));
            record.addLabelValue(IP, getIPAddress(conf));
        } catch (UnknownHostException ignored) {
            // do nothing as this fields are optional
        }

        record.addLabelValue(ROBOTS, "classic"); // TODO Make configurable?
        record.addLabelValue(OPERATOR, conf.get("http.agent.email", ""));

        return record;
    }

    public static final String getHostname(Configuration conf)
            throws UnknownHostException {

        return StringUtil.isEmpty(conf.get("http.agent.host", "")) ?
                InetAddress.getLocalHost().getHostName() :
                conf.get("http.agent.host");
    }

    public static final String getIPAddress(Configuration conf)
            throws UnknownHostException {

        return InetAddress.getLocalHost().getHostAddress();
    }

    public static final byte[] toByteArray(HttpHeaders headers)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        headers.write(out);

        return out.toByteArray();
    }

    public static final String getAgentString(String name, String version,
            String description, String URL, String email) {

        StringBuffer buf = new StringBuffer();

        buf.append(name);

        if (version != null) {
            buf.append("/").append(version);
        }

        if (((description != null) && (description.length() != 0)) || (
                (email != null) && (email.length() != 0)) || ((URL != null) && (
                URL.length() != 0))) {
            buf.append(" (");

            if ((description != null) && (description.length() != 0)) {
                buf.append(description);
                if ((URL != null) || (email != null))
                    buf.append("; ");
            }

            if ((URL != null) && (URL.length() != 0)) {
                buf.append(URL);
                if (email != null)
                    buf.append("; ");
            }

            if ((email != null) && (email.length() != 0))
                buf.append(email);

            buf.append(")");
        }

        return buf.toString();
    }

    public static final WARCRecordInfo docToMetadata(NutchDocument doc)
            throws UnsupportedEncodingException {
        WARCRecordInfo record = new WARCRecordInfo();

        record.setType(WARCConstants.WARCRecordType.metadata);
        record.setUrl((String) doc.getFieldValue("id"));
        record.setCreate14DigitDate(
                DateUtils.get14DigitDate((Date) doc.getFieldValue("tstamp")));
        record.setMimetype("application/warc-fields");
        record.setRecordId(generator.getRecordID());

        // metadata
        ANVLRecord metadata = new ANVLRecord();

        for (String field : doc.getFieldNames()) {
            List<Object> values = doc.getField(field).getValues();
            for (Object value : values) {
                if (value instanceof Date) {
                    metadata.addLabelValue(field, DateUtils.get14DigitDate());
                } else {
                    metadata.addLabelValue(field, (String) value);
                }
            }
        }

        record.setContentLength(metadata.getLength());
        record.setContentStream(
                new ByteArrayInputStream(metadata.getUTF8Bytes()));

        return record;
    }
}
