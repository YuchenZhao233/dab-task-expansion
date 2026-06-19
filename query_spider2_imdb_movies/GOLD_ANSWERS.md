# Gold Answers

## Query 1

Among English-language movies officially released in 2018, which five directors accumulated the most total votes across movies they directed? Consider only directors with at least two such movies. Return each director's person catalog ID, name, movie count, average rating rounded to 2 decimals, total votes, and total duration in minutes; order by total votes descending.

person_catalog_id,director_name,movie_count,avg_rating,total_votes,total_duration
person-card:0744834,Eli Roth,2,6.20,91852,212
person-card:0573732,Sean McNamara,2,6.25,4001,202
person-card:5141259,Fabien Delage,2,6.90,2407,181
person-card:3163561,Rene Perez,2,4.40,1475,164
person-card:4335588,Jamie Patterson,2,4.95,1177,166

## Query 2

Determine the three genres with the most movies rated above 8.0. Within movies belonging to any of those three genres, list the four directors who directed the most above-8.0 movies. Return the genre set used plus each director's person catalog ID, name, and movie count; order directors by count descending and then name ascending.

top_genres,person_catalog_id,director_name,movie_count
Drama|Action|Comedy,person-card:0751577,Anthony Russo,2
Drama|Action|Comedy,person-card:0003506,James Mangold,2
Drama|Action|Comedy,person-card:0751648,Joe Russo,2
Drama|Action|Comedy,person-card:2765738,Marianne Elliott,2

## Query 3

For each official release year, among exact country labels with at least 10 movies that each received at least 20,000 votes, which country label has the highest average rating? Return the year, country label, movie count, average rating rounded to 2 decimals, and total votes, ordered by year.

year,country_label,movie_count,avg_rating,total_votes
2017,USA,74,6.51,7639547
2018,UK, USA,15,6.78,1912644
2019,USA,49,6.68,3833768

## Query 4

Which five performers born before 1970 have the most appearances in Horror or Thriller movies rated below 5.0? Return each performer's person catalog ID, name, matching movie count, and average rating rounded to 2 decimals; order by movie count descending, then average rating descending, then name ascending.

person_catalog_id,performer_name,movie_count,avg_rating
person-card:0000616,Eric Roberts,4,3.80
person-card:0001744,Tom Sizemore,3,4.23
person-card:0000185,Dolph Lundgren,3,4.10
person-card:0865302,Tony Todd,3,3.23
person-card:0000448,Lance Henriksen,3,3.03

## Query 5

For non-English Drama movies with at least 1,000 votes, which five production companies have the highest worldwide gross total? Return production company, movie count, total gross income as a number, and average rating rounded to 2 decimals; order by total gross descending.

production_company,movie_count,total_gross_income,avg_rating
Arka Mediaworks,1,254158390,8.20
Huayi Brothers Pictures,1,227091290,7.10
Dexter Studios,2,207346210,7.20
Shanghai Professional Making Film,1,151092784,6.50
Aamir Khan Productions,1,121956937,7.90

## Query 6

Which directors have at least one directed movie in each official release year 2017, 2018, and 2019, with their average movie rating strictly increasing from 2017 to 2018 to 2019? Return each director's person catalog ID, name, the three yearly average ratings rounded to 2 decimals, and the directed movie count for each year.

person_catalog_id,director_name,avg_2017,avg_2018,avg_2019,count_2017,count_2018,count_2019
person-card:0425364,Jesse V. Johnson,4.55,6.20,6.50,2,1,1

## Query 7

For non-English movies officially released in 2018 and published in October through December, which genre has the highest vote-weighted average rating among genres with at least 5 matching movies? Return the genre, movie count, total votes, and vote-weighted average rating rounded to 2 decimals.

genre,movie_count,total_votes,vote_weighted_avg_rating
Crime,32,111720,7.73

## Query 8

Among directors whose person profile lists known-for movies that are also in this dataset, compare their average rating on directed movies outside that known-for list with the average rating of their known-for movies. Considering only directors with at least two directed movies outside the known-for list, which five directors have the largest positive rating gap? Return person catalog ID, name, outside-list directed movie count, known-for movie count, outside-list average rating, known-for average rating, and the gap, with averages rounded to 2 decimals.

person_catalog_id,director_name,outside_directed_count,known_for_count,outside_avg_rating,known_for_avg_rating,rating_gap
person-card:0838289,Peter Sullivan,2,2,5.50,4.35,1.15
person-card:1655252,Joel Paul Reisig,2,1,4.85,4.00,0.85
person-card:0784805,Giorgio Serafini,2,1,3.95,3.20,0.75
person-card:1729447,Onur Ünlü,3,1,5.80,5.20,0.60
person-card:1953143,Steven M. Smith,2,3,2.90,2.33,0.57
