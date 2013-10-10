package org.apache.nutch.scoring;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

public abstract class AbstractScoringFilter implements ScoringFilter {

	private Configuration conf;

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public void injectedScore(Text url, CrawlDatum datum)
			throws ScoringFilterException {
	}

	public void initialScore(Text url, CrawlDatum datum)
			throws ScoringFilterException {
	}

	public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
			throws ScoringFilterException {
		return initSort;
	}

	public void passScoreBeforeParsing(Text url, CrawlDatum datum,
			Content content) throws ScoringFilterException {
	}

	public void passScoreAfterParsing(Text url, Content content, Parse parse)
			throws ScoringFilterException {
	}

	public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
			ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
			CrawlDatum adjust, int allCount) throws ScoringFilterException {
		return adjust;
	}

	public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
			List<CrawlDatum> inlinked) throws ScoringFilterException {
	}

	@Override
	public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
			CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
			throws ScoringFilterException {
		return initScore;
	}

}
