# Cassandra, Weather Data
## Overview
NOAA (National Oceanic and Atmospheric Administration) collects weather data from all over the world. In this project, I explore how (1) to store this data in Cassandra, (2) write a server for data collection, and (3) analyze the collected data via Spark.

I'll also explore read/write availability tradeoffs. We always want sensors to be able to upload data, but it is OK if we cannot always read the latest stats (we prefer an error over inconsistent results).

Objectives:

- create a schema for a Cassandra table that uses a partition key, cluster key, and static column
- configure Spark catalogs to gain access to external data sources
- create custom Cassandra types
- create custom Spark UDFs (user defined functions)
- configure queries to tradeoff read/write availability
- refresh a stale cache
