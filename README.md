# Chicago Car Accidents Analysis API

## Overview

This project provides an API to analyze and query car accident data in Chicago. It processes accident data from a CSV file, aggregates it into MongoDB collections, and offers several APIs to retrieve accident statistics by various factors such as time period, location, and cause. MongoDB indexes are used to optimize query performance, and execution time comparisons with and without indexes are demonstrated.

### Key Features:
- **Accident Statistics**: Aggregates accidents by day, week, and month.
- **Accidents Grouped by Cause**: Analyze accidents based on the primary cause.
- **Injury Statistics**: Fetch injury data, including fatal and non-fatal injuries for specific areas.
- **Indexing and Performance**: MongoDB indexing is used to improve query performance. Comparisons of query execution times with and without indexing are provided.


### command for start
- **docker container run -d -p 27017:27017 --name mongodb mongo
- **docker exec -it mongodb mongosh
