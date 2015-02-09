CODING EXERCISE​:
================
A water network is composed of a hierarchical structure of water supply zones. 
Attached is a file describing this hierarchy (Supply zones.csv). 
These supply zones have meter reading values for a certain week in January 
as listed in the attached file (Supply values.csv). 

You do not need to validate the correctness of the input files.

Please provide the supply values for all possible zones and dates according to the following preference:

If the zone has an actual value then provide it and mark value as "actual".
If all direct children of the zone have a value for a certain date 
sum these values and mark value as "aggregated"

The output should have the following columns: zone name, date, value, type (actual/aggregated).