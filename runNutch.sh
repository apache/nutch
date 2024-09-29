#/bin/bash
echo "Will remove existing CrawlDb..."
sleep 5
echo "Removing existing CrawlDb..."
rm -rf crawl/*

./runtime/local/bin/nutch inject crawl/crawldb urls

while true
do
  ./runtime/local/bin/nutch generate crawl/crawldb crawl/segments/
  segment=`ls crawl/segments/ | tail -1`
  echo "Found segment $segment"
  sleep 5
  if [ "$?" == "0" ] && [ ! -z "$segment" ]
  then
    ./runtime/local/bin/nutch fetch crawl/segments/$segment
    if [ "$?" == "0" ]
    then
      sleep 5
      ./runtime/local/bin/nutch parse crawl/segments/$segment
      sleep 5
      ./runtime/local/bin/nutch updatedb crawl/crawldb crawl/segments/$segment
      sleep 5
      ./runtime/local/bin/nutch index crawl/crawldb crawl/segments/$segment
      sleep 10
      rm -rf crawl/segments/$segment
    fi
  else
    sleep 30
  fi
done