#!/usr/bin/env bash

FIRST_DATE=2016-02-01
LAST_DATE=2016-12-31
INPUT_TABLE=world-fishing-827:pipeline_classify_p_p516_daily.
OUTPUT_TABLE=world-fishing-827:scratch_name_normalize.classify_p516_


# on osx, use gdate which is installed with `brew install coreutils`
case "$OSTYPE" in
  darwin*)  DATE=gdate ;;
  *)        DATE=date ;;
esac

# date_range(from_date, to_date)
# return a list for dates from from_date to to_date, inclusive
#
# Usage
# for date in $(date_range 2015-01-01 2015-01-04); do
#    echo $date
# done

date_range() {
    local d=$1
    echo $d
    while [ "$d" != $2 ]; do
      d=$(${DATE} -I -d "$d + 1 day")
      echo $d
    done
}

# yyyymmdd(date)
# formats a date as YYYYmmdd
yyyymmdd() {
    echo $(${DATE} +%Y%m%d -d $1)

}

for dt in $(date_range ${FIRST_DATE} ${LAST_DATE}); do
    YYYYMMDD=$(yyyymmdd $dt)
    SRC_TABLE=${INPUT_TABLE}${YYYYMMDD}
    DEST_TABLE=${OUTPUT_TABLE}${YYYYMMDD}
    echo "Copying ${SRC_TABLE} ==> ${DEST_TABLE}"

    bq cp ${SRC_TABLE} ${DEST_TABLE}

    done
