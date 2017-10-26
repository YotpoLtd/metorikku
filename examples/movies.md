# Examples

Metorikku is a library that simplifies writing and executing ETLs on top of [Apache Spark](http://spark.apache.org/).
A user needs to write a simple JSON configuration file that includes SQL queries and run Metorikku on a spark cluster.
The platform also includes a way to write tests for metrics using MetorikkuTester.

### Getting started
To run Metorikku you must first define 2 files.

##### MQL file
An MQL (Metorikku Query Language) file defines the steps and queries of the ETL as well as where and what to output.

For example a simple configuration JSON should be as follows:
```
+------+-------+------+----------+----------------------------------------------------------------------------+-----------------------------------------------+
|userId|movieId|rating|timestamp |title                                                                       |genres                                         |
+------+-------+------+----------+----------------------------------------------------------------------------+-----------------------------------------------+
|1     |31     |2.5   |1260759144|Dangerous Minds (1995)                                                      |Drama                                          |
|1     |1029   |3.0   |1260759179|Dumbo (1941)                                                                |Animation|Children|Drama|Musical               |
|1     |1061   |3.0   |1260759182|Sleepers (1996)                                                             |Thriller                                       |
|1     |1129   |2.0   |1260759185|Escape from New York (1981)                                                 |Action|Adventure|Sci-Fi|Thriller               |
|1     |1172   |4.0   |1260759205|Cinema Paradiso (Nuovo cinema Paradiso) (1989)                              |Drama                                          |
|1     |1263   |2.0   |1260759151|Deer Hunter, The (1978)                                                     |Drama|War                                      |
|1     |1287   |2.0   |1260759187|Ben-Hur (1959)                                                              |Action|Adventure|Drama                         |
|1     |1293   |2.0   |1260759148|Gandhi (1982)                                                               |Drama                                          |
|1     |1339   |3.5   |1260759125|Dracula (Bram Stoker's Dracula) (1992)                                      |Fantasy|Horror|Romance|Thriller                |
|1     |1343   |2.0   |1260759131|Cape Fear (1991)                                                            |Thriller                                       |
|1     |1371   |2.5   |1260759135|Star Trek: The Motion Picture (1979)                                        |Adventure|Sci-Fi                               |
|1     |1405   |1.0   |1260759203|Beavis and Butt-Head Do America (1996)                                      |Adventure|Animation|Comedy|Crime               |
|1     |1953   |4.0   |1260759191|French Connection, The (1971)                                               |Action|Crime|Thriller                          |
|1     |2105   |4.0   |1260759139|Tron (1982)                                                                 |Action|Adventure|Sci-Fi                        |
|1     |2150   |3.0   |1260759194|Gods Must Be Crazy, The (1980)                                              |Adventure|Comedy                               |
|1     |2193   |2.0   |1260759198|Willow (1988)                                                               |Action|Adventure|Fantasy                       |
```
Take a look at the [examples file](http://test.com) for further configuration examples.