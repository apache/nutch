# example of domain configuration for CloudSearch

DOMAIN="$1"

if [ "$DOMAIN" = "" ]; then
    echo "Need to specify a domain name as argument"
    exit -1;
fi

aws cloudsearch create-domain --domain-name $DOMAIN

aws cloudsearch define-index-field --domain-name $DOMAIN --name boost --type double --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name content --type text --sort-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name digest --type literal --sort-enabled false --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name host --type literal --sort-enabled false --facet-enabled true
aws cloudsearch define-index-field --domain-name $DOMAIN --name id --type literal --sort-enabled false --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name segment --type literal --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name title --type text --sort-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name tstamp --type date --sort-enabled true --facet-enabled false
aws cloudsearch define-index-field --domain-name $DOMAIN --name url --type literal --sort-enabled false --facet-enabled false


