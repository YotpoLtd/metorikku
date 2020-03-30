# Examples

### Running the example
`java -Dspark.master=local[*] -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku -c examples/movies.yaml`

### The Data
Let's analyze a small movie lens data set!

We have our movies.csv in our file_inputs folder 
```
+-------+-------------------------------------------------------------------------------+-------------------------------------------+
|movieId|title                                                                          |genres                                     |
+-------+-------------------------------------------------------------------------------+-------------------------------------------+
|1      |Toy Story (1995)                                                               |Adventure|Animation|Children|Comedy|Fantasy|
|2      |Jumanji (1995)                                                                 |Adventure|Children|Fantasy                 |
|3      |Grumpier Old Men (1995)                                                        |Comedy|Romance                             |
|4      |Waiting to Exhale (1995)                                                       |Comedy|Drama|Romance                       |
|5      |Father of the Bride Part II (1995)                                             |Comedy                                     |
|6      |Heat (1995)                                                                    |Action|Crime|Thriller                      |
|7      |Sabrina (1995)                                                                 |Comedy|Romance                             |
|8      |Tom and Huck (1995)                                                            |Adventure|Children                         |
|9      |Sudden Death (1995)                                                            |Action                                     |
|10     |GoldenEye (1995)                                                               |Action|Adventure|Thriller                  |
|11     |American President, The (1995)                                                 |Comedy|Drama|Romance                       |
|12     |Dracula: Dead and Loving It (1995)                                             |Comedy|Horror                              |
|13     |Balto (1995)                                                                   |Adventure|Animation|Children               |
|14     |Nixon (1995)                                                                   |Drama                                      |
|15     |Cutthroat Island (1995)                                                        |Action|Adventure|Romance                   |

```
and also our ratings.csv:
```
+------+-------+------+----------+
|userId|movieId|rating|timestamp |
+------+-------+------+----------+
|1     |31     |2.5   |1260759144|
|1     |1029   |3.0   |1260759179|
|1     |1061   |3.0   |1260759182|
|1     |1129   |2.0   |1260759185|
|1     |1172   |4.0   |1260759205|
|1     |1263   |2.0   |1260759151|
|1     |1287   |2.0   |1260759187|
|1     |1293   |2.0   |1260759148|
|1     |1339   |3.5   |1260759125|
|1     |1343   |2.0   |1260759131|
|1     |1371   |2.5   |1260759135|
|1     |1405   |1.0   |1260759203|
|1     |1953   |4.0   |1260759191|
|1     |2105   |4.0   |1260759139|
|1     |2150   |3.0   |1260759194|
|1     |2193   |2.0   |1260759198|
```
### Configuration 
We are registering our data sources, our variables and our output configurations  
Here's our example configuration:
```yaml
# The MQL file path
metrics:
  - examples/movies_metric.yaml

inputs:
 movies: examples/file_inputs/movies.csv
 ratings: examples/file_inputs/ratings.csv

# Set custom variables that would be accessible from the SQL
variables:
 myFavoriteMovie: 'Princess Bride, The (1987)'

output:
  file:
    dir: examples/output

# If set to true, triggers Explain before saving
explain: true

# Set Log Level : ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
logLevel: WARN

# Set Application Name to have app name prefix in spark instrumentation counters
appName: moviesApp

# Shows a Preview of the output
showPreviewLines: 100
```
### The Metric
Our metric file is as follows:
```yaml
steps:
- dataFrameName: moviesWithRatings
  sql:
    SELECT userid,
           movies.movieid,
           rating,
           timestamp,
           title,
           genres
    FROM ratings
    JOIN movies ON ratings.movieid = movies.movieid
- dataFrameName: fantasyMoviesWithRatings
  sql:
    SELECT movieId,
           cast(rating AS float) AS rating,
           timestamp,
           title,
           genres
    FROM moviesWithRatings
    WHERE genres LIKE '%Fantasy%'
- dataFrameName: topFantasyMovies
  sql:
    SELECT movieId,
           title,
           avg(rating) AS averageRating
    FROM fantasyMoviesWithRatings
    GROUP BY movieId,
             title
    ORDER BY averageRating DESC
    LIMIT 100
- dataFrameName: myFavoriteMovieRated
  sql:
    SELECT *
    FROM topFantasyMovies
    WHERE title = ${myFavoriteMovie}
output:
- dataFrameName: topFantasyMovies
  outputType: Parquet
  outputOptions:
    saveMode: Overwrite
    path: topFantasyMovies.parquet
```
### Results
We are running each step sequentially and here are the results:   
#### Step1 - moviesWithRatings
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
|1     |1405   |1.0   |1260759203|Beavis and Butt-Head Do America (1996)                                      |Adventure|Animation|Comedy|Crime            
```
#### Step2 - fantasyMoviesWithRatings
```
+-------+------+----------+----------------------------------------------------------------------------------------------+---------------------------------------------------------+
|movieId|rating|timestamp |title                                                                                         |genres                                                   |
+-------+------+----------+----------------------------------------------------------------------------------------------+---------------------------------------------------------+
|1339   |3.5   |1260759125|Dracula (Bram Stoker's Dracula) (1992)                                                        |Fantasy|Horror|Romance|Thriller                          |
|2193   |2.0   |1260759198|Willow (1988)                                                                                 |Action|Adventure|Fantasy                                 |
|2294   |2.0   |1260759108|Antz (1998)                                                                                   |Adventure|Animation|Children|Comedy|Fantasy              |
|2968   |1.0   |1260759200|Time Bandits (1981)                                                                           |Adventure|Comedy|Fantasy|Sci-Fi                          |
|265    |5.0   |835355697 |Like Water for Chocolate (Como agua para chocolate) (1992)                                    |Drama|Fantasy|Romance                                    |
|314    |4.0   |835356044 |Secret of Roan Inish, The (1994)                                                              |Children|Drama|Fantasy|Mystery                           |
|317    |2.0   |835355551 |Santa Clause, The (1994)                                                                      |Comedy|Drama|Fantasy                                     |
|367    |3.0   |835355619 |Mask, The (1994)                                                                              |Action|Comedy|Crime|Fantasy                              |
|405    |2.0   |835356246 |Highlander III: The Sorcerer (a.k.a. Highlander: The Final Dimension) (1994)                  |Action|Fantasy                                           |
|410    |3.0   |835355532 |Addams Family Values (1993)                                                                   |Children|Comedy|Fantasy                                  |
|485    |3.0   |835355918 |Last Action Hero (1993)                                                                       |Action|Adventure|Comedy|Fantasy                          |
|551    |5.0   |835355767 |Nightmare Before Christmas, The (1993)                                                        |Animation|Children|Fantasy|Musical                       |
|587    |3.0   |835355779 |Ghost (1990)                                                                                  |Comedy|Drama|Fantasy|Romance|Thriller                    |
|661    |4.0   |835356141 |James and the Giant Peach (1996)                                                              |Adventure|Animation|Children|Fantasy|Musical             |
            
```
#### Step3 - topFantasyMovies
```
+-------+------------------------------------------------------------------------------------------------------------------+------------------+
|movieId|title                                                                                                             |averageRating     |
+-------+------------------------------------------------------------------------------------------------------------------+------------------+
|59392  |Stargate: The Ark of Truth (2008)                                                                                 |5.0               |
|3216   |Vampyros Lesbos (Vampiras, Las) (1971)                                                                            |5.0               |
|140747 |16 Wishes (2010)                                                                                                  |5.0               |
|3837   |Phantasm II (1988)                                                                                                |5.0               |
|27792  |Saddest Music in the World, The (2003)                                                                            |5.0               |
|118468 |Mei and the Kittenbus (2002)                                                                                      |5.0               |
|2086   |One Magic Christmas (1985)                                                                                        |5.0               |
|8254   |Arizona Dream (1993)                                                                                              |5.0               |
|4789   |Phantom of the Paradise (1974)                                                                                    |5.0               |
|74089  |Peter Pan (1960)                                                                                                  |5.0               |
|96832  |Holy Motors (2012)                                                                                                |5.0               |
|106471 |One Piece Film: Strong World (2009)                                                                               |5.0               |
|101962 |Wolf Children (Okami kodomo no ame to yuki) (2012)                                                                |5.0               |
|3612   |The Slipper and the Rose: The Story of Cinderella (1976)                                                          |5.0               |
|4591   |Erik the Viking (1989)                                                                                            |5.0               |
|7302   |Thief of Bagdad, The (1924)                                                                                       |5.0               |
|26749  |Prospero's Books (1991)                                                                                           |5.0               |
|99764  |It's Such a Beautiful Day (2012)                                                                                  |5.0               |
|53887  |O Lucky Man! (1973)                                                                                               |5.0               |
|95113  |Eaux d'artifice (1953)                                                                                            |5.0               |
|72356  |Partly Cloudy (2009)                                                                                              |4.75              |
|50641  |House (Hausu) (1977)                                                                                              |4.75              |
|114552 |Boxtrolls, The (2014)                                                                                             |4.5               |
|62764  |Black Moon (1975)                                                                                                 |4.5               |
|136016 |The Good Dinosaur (2015)                                                                                          |4.5               |
|80748  |Alice in Wonderland (1933)                                                                                        |4.5               |
|110645 |Witching and Bitching (Brujas de Zugarramurdi, Las) (2014)                                                        |4.5               |
|27156  |Neon Genesis Evangelion: The End of Evangelion (Shin seiki Evangelion Gekijô-ban: Air/Magokoro wo, kimi ni) (1997)|4.5               |
|50011  |Bothersome Man, The (Brysomme mannen, Den) (2006)                                                                 |4.5               |
|68835  |Were the World Mine (2008)                                                                                        |4.5               |
|4927   |Last Wave, The (1977)                                                                                             |4.5               |
```            
#### Step2 - myFavoriteMovieRated
```
+-------+--------------------------+-----------------+
|movieId|title                     |averageRating    |
+-------+--------------------------+-----------------+
|1197   |Princess Bride, The (1987)|4.208588957055214|
+-------+--------------------------+-----------------+
```

## Testing our metric

### Running the test
`java -Dspark.master=local[*] -cp metorikku-standalone.jar com.yotpo.metorikku.MetorikkuTester -t examples/movies_test.yaml`

Metorikku also supports testing your logic, using MetorikkuTester.

Metorikku Tester expects a test-settings YAML file:

```yaml
metric: movies_metric.yaml
mocks:
- name: movies
  path: mocks/movies.jsonl
- name: ratings
  path: mocks/ratings.jsonl
params:
  variables:
    myFavoriteMovie: Lord of the Rings, The (1978)
tests:
  myFavoriteMovieRated:
  - movieId: 1
    title: Lord of the Rings, The (1978)
    averageRating: 2.5
``` 
A test settings file consists of the following:
* Our metric file which has our business logic
* A set of mocks in the format of **JSONL** 
* A set of variables if needed to be used inside our SQL queries
* A set of expected results

You can run MetorikkuTester as a stand alone application or a spark application