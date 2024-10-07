#/bin/bash

if [ -z "$JAVA_HOME" ]
then
  echo ERROR: JAVA_HOME is not set.
  exit 1
fi


echo "Will remove existing CrawlDb..."
sleep 5
echo "Removing existing CrawlDb..."
banner "Delete DB"
rm -rf crawl/* || exit 1
docker exec -it solr_nutch solr delete -c nutch || exit 1

banner "Inject URLs"
./runtime/local/bin/nutch inject crawl/crawldb urls

banner "Create Solr"
cp src/plugin/indexer-solr/schema.xml solr_datadir
docker exec -it solr_nutch cp /opt/solr-9.7.0/server/solr/configsets/_default/conf/solrconfig.xml /var/solr/data/nutch
docker exec -it solr_nutch cp /opt/solr-9.7.0/server/solr/configsets/_default/conf/stopwords.txt /var/solr/data/nutch
docker exec -it solr_nutch cp /opt/solr-9.7.0/server/solr/configsets/_default/conf/protwords.txt /var/solr/data/nutch
docker exec -it solr_nutch cp /opt/solr-9.7.0/server/solr/configsets/_default/conf/synonyms.txt /var/solr/data/nutch
docker exec -it solr_nutch solr create_core -c nutch -d /var/solr/data/nutch || exit 1

while true
do
  sleep 5
  banner Generate Segment
  ./runtime/local/bin/nutch generate crawl/crawldb crawl/segments/
  segment=`ls crawl/segments/ | tail -1`
  echo "Found segment $segment"
  sleep 5
  if [ "$?" == "0" ] && [ ! -z "$segment" ]
  then
    banner "Fetch"
    ./runtime/local/bin/nutch fetch crawl/segments/$segment
    if [ "$?" == "0" ]
    then
      sleep 5
      banner "Parse"
      ./runtime/local/bin/nutch parse crawl/segments/$segment
      sleep 5
      banner UpdateDB
      ./runtime/local/bin/nutch updatedb crawl/crawldb crawl/segments/$segment
      sleep 5
      banner Index
      ./runtime/local/bin/nutch index crawl/crawldb crawl/segments/$segment
      sleep 10
      rm -rf crawl/segments/$segment
    fi
  else
    exit 5
  fi
done