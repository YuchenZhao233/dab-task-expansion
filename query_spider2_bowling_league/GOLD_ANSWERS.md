# BowlingLeague Gold Answers

## Query 1

Which bowlers won games with handicap scores of 190 or less at all three venues: Thunderbird Lanes, Totem Lanes, and Bolero Lanes? Return only the qualifying game records at those three venues with bowler roster ID, first name, last name, match score ID, game code, handicap score, tournament date, and venue. Row order is not important.

bowler_id,first_name,last_name,match_score_id,game_code,handicap_score,tournament_date,venue
roster-bowler-013,Elizabeth,Hallmark,score-match-010,G1,189,2017-09-18,Bolero Lanes
roster-bowler-013,Elizabeth,Hallmark,score-match-024,G3,190,2017-10-09,Totem Lanes
roster-bowler-013,Elizabeth,Hallmark,score-match-034,G1,189,2017-10-30,Thunderbird Lanes
roster-bowler-025,Megan,Patterson,score-match-007,G1,188,2017-09-11,Thunderbird Lanes
roster-bowler-025,Megan,Patterson,score-match-021,G1,189,2017-10-09,Totem Lanes
roster-bowler-025,Megan,Patterson,score-match-035,G1,187,2017-10-30,Thunderbird Lanes
roster-bowler-025,Megan,Patterson,score-match-039,G2,181,2017-11-06,Bolero Lanes
roster-bowler-019,John,Viescas,score-match-007,G3,185,2017-09-11,Thunderbird Lanes
roster-bowler-019,John,Viescas,score-match-012,G1,181,2017-09-18,Bolero Lanes
roster-bowler-019,John,Viescas,score-match-036,G1,179,2017-10-30,Thunderbird Lanes
roster-bowler-019,John,Viescas,score-match-052,G2,185,2017-11-27,Totem Lanes

## Query 2

For each tournament venue, which team recorded the most match-game wins? Break ties alphabetically by team name to choose the venue winner. Return venue, team roster ID, team name, and win count. Row order is not important.

venue,team_id,team_name,win_count
Acapulco Lanes,roster-team-006,Orcas,4
Bolero Lanes,roster-team-005,Dolphins,4
Imperial Lanes,roster-team-004,Barracudas,4
Red Rooster Lanes,roster-team-007,Manatees,4
Sports World Lanes,roster-team-005,Dolphins,4
Thunderbird Lanes,roster-team-004,Barracudas,4
Totem Lanes,roster-team-004,Barracudas,4

## Query 3

Among registered cities with at least three ZIP-confirmed bowlers who have game score records, which cities have a city-level average of individual bowler season raw-score averages above the roster current average for those same scoring bowlers? Return city, confirmed scoring-bowler count, average roster current average, average of individual bowler season raw-score averages, and raw-minus-current difference, rounded to 2 decimals. Row order is not important.

city,confirmed_scoring_bowler_count,avg_current_average,avg_raw_score,raw_minus_current
Auburn,5,155.20,155.31,0.11

## Query 4

Among teams that recorded match-game wins at every tournament venue, identify the top five teams by how many low-handicap games their captain personally won, where low-handicap means a winning handicap score of 190 or less. Use total team match-game wins descending, then team name ascending, only to break ties for inclusion in the top five. Return team ID, team name, captain token from the team registry, captain name, venue count, team win count, captain low-win count, and the captain's average handicap score in those low wins rounded to 2 decimals. Row order is not important.

team_id,team_name,captain_token,captain_name,venue_count,team_win_count,captain_low_win_count,captain_avg_low_handicap
roster-team-004,Barracudas,captain-bowler-016,Richard Sheskey,7,21,4,188.75
roster-team-001,Marlins,captain-bowler-002,David Fournier,7,21,4,181.00
roster-team-005,Dolphins,captain-bowler-020,Suzanne Viescas,7,21,3,189.00
roster-team-007,Manatees,captain-bowler-028,Michael Viescas,7,21,3,188.33
roster-team-002,Sharks,captain-bowler-005,Ann Patterson,7,21,2,188.00

## Query 5

Which five bowlers exceeded their roster current average at the most tournament venues, where a venue counts only if the bowler's average raw score at that venue is greater than the current average? Use largest positive venue gap descending, then last name and first name, only to break ties for inclusion in the five bowlers. Return bowler ID, first name, last name, venues above current, largest positive venue gap rounded to 2 decimals, and average of venue raw averages across all venues where the bowler has game scores, not only the above-current venues, rounded to 2 decimals. Row order is not important.

bowler_id,first_name,last_name,venues_above_current,largest_positive_venue_gap,avg_venue_raw_average
roster-bowler-018,Michael,Hernandez,5,3.50,157.02
roster-bowler-003,John,Kennedy,4,11.17,165.62
roster-bowler-001,Barbara,Fournier,4,9.33,148.62
roster-bowler-007,David,Viescas,4,9.17,167.67
roster-bowler-011,Angel,Kennedy,4,7.50,163.36

## Query 6

For each tournament month, which team had the most match-game wins? Break ties alphabetically by team name. Return month, team ID, team name, win count, and number of tournaments held that month. Row order is not important.

month,team_id,team_name,win_count,tournament_count
2017-09,roster-team-005,Dolphins,7,4
2017-10,roster-team-006,Orcas,9,5
2017-11,roster-team-004,Barracudas,6,4
2017-12,roster-team-004,Barracudas,2,1

