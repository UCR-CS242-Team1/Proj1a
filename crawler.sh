#!/bin/sh

if [ -z "$1" ]; then
	TWEETS=0
else
	TWEETS=$1
fi
if [ -z "$2" ]; then
	mkdir -p data
	FILEPATH="data"
else
	mkdir -p $2
	FILEPATH=$2
fi

echo "Scraping Twitter data" 
python "twitterScraper.py" $TWEETS $FILEPATH 