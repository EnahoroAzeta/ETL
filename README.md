# ETL Pipeline for Relational Database

# Introduction

The goal of this project is to build an ETL pipleine for a PostgreSQL database utilizing the data on users activity and songs metadata. The database helps us do complex analytics regarding users activity as well as song play analysis.

# Data
The songs' metadata source is a subset of the Million Song Dataset. Also, the users' activities is a simulated data using eventsim. The data resides in two main directories:

  - Songs metadata: collection of JSON files that describes the songs such as title, artist name, year, etc.
  - Logs data: collection of JSON files where each file covers the users activities over a given day.


# Procedure
The database will be designed and opimized for complex queries. To do that, Star schema will be used utilizing dimensional modeling as follows:

    Fact table: songplays.
    Dimensions tables: songs, artist, users, time.

The following objectives are to be met:

Denormalized tables.
Simplified queries.
Fast aggregation.

<img width="683" alt="Star Schema" src="https://user-images.githubusercontent.com/85859888/121840558-6dab2d80-ccd4-11eb-9b01-5ad9a916eb46.png">

