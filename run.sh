#!/bin/sh

wd='/home/jteruya/nowbutton_analysis'

if [ "$1" = "cleanup" ]
then
   echo `date` cleanup_staging_tables.sql; psql -h 10.208.97.116 -p 5432 etl analytics -f "$wd/sql/cleanup_staging_tables.sql"
   exit 0
fi


# Run SQL and Generate CSV Files
echo `date` nowbutton_analysis.sql; psql -h 10.208.97.116 -p 5432 etl analytics -f "$wd/sql/nowbutton_analysis.sql"

# Archive CSV Files
cp $wd/csv/event_level_summary.csv $wd/csv/archive/event_level_summary_$(date +"%Y-%m-%d_%H-%M-%S").csv
cp $wd/csv/ios_event_level_summary.csv $wd/csv/archive/ios_event_level_summary_$(date +"%Y-%m-%d_%H-%M-%S").csv
cp $wd/csv/android_event_level_summary.csv $wd/csv/archive/android_event_level_summary_$(date +"%Y-%m-%d_%H-%M-%S").csv
