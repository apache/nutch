package org.apache.nutch.scoring.similarity;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimilarityScoringFilter implements ScoringFilter {

  private Configuration conf;
  private String goldStandardDocPath;
  private final static Logger LOG = LoggerFactory
      .getLogger(SimilarityScoringFilter.class);
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    goldStandardDocPath = conf.get("similarity.model.path");
    LOG.info("Getting the goldstanrd path {}",goldStandardDocPath);
  }

  @Override
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
      throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
    CosineSimilarity cs = new CosineSimilarity();
    File f = new File(goldStandardDocPath);
    String goldStandard = "";
      try {
        goldStandard = FileUtils.readFileToString(f);
      } catch (IOException e) {
        e.printStackTrace();
      }
      double parseTextSimilarity = cs.calculateCosineSimilarity(goldStandard, parse.getText());
      LOG.info("Calculating similarity between golddoc and {}",url);
      double metaKeywordSimilarity = cs.calculateCosineSimilarity(goldStandard, parse.getData().getParseMeta().get("metatag.keyword"));
      double metaDescriptionSimilarity = cs.calculateCosineSimilarity(goldStandard, parse.getData().getParseMeta().get("metatag.description"));
      int count = 0;
      if(parseTextSimilarity!=0)
        count++;
      if(metaDescriptionSimilarity!=0)
        count++;
      if(metaKeywordSimilarity!=0)
        count++;
      if(count==0)
        count++;
      
      double score =  (parseTextSimilarity+metaDescriptionSimilarity + metaKeywordSimilarity)/count;
      System.out.println("Score of the url " + url + " is : " + score);
      LOG.info("Setting score of {} to {}",url, score);
      LOG.info("Score break down TextSimilarity : {}, metaKeywordSimilarity : {}, metaDescriptionSimilarity : {}",
          parseTextSimilarity, metaKeywordSimilarity, metaDescriptionSimilarity);
      parse.getData().getContentMeta()
      .set(Nutch.SCORE_KEY, score+"");
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
      float score = Float.parseFloat(parseData.getContentMeta().get(Nutch.SCORE_KEY));
      for (Entry<Text, CrawlDatum> target : targets) {
        target.getValue().setScore((float)score);
        LOG.info("Setting score of {} to {}",target.getValue(), score);
      }
      
    
    return adjust;
  }

  @Override
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    // TODO Auto-generated method stub
    
    return 0;
  }

}
