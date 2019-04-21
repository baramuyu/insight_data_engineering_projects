#!/bin/sh
AWEEKAGO=`date --date="7 day ago" +%m%d%Y`
curl -o $AWEEKAGO.csv "http://web6.seattle.gov/SDOT/wapiParkingStudy/api/ParkingTransaction?from=$AWEEKAGO&to=$AWEEKAGO"
