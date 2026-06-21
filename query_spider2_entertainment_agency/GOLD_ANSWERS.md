# EntertainmentAgency Gold Answers

The answer files were computed from `clean/clean.sqlite` using `manual_querycode/compute_ground_truth.py`.

## Query 1

Which musical styles look under-supplied by active entertainers? Use weighted customer demand (3 points for first preference, 2 for second, 1 for third) and compare it with the sum of style-strength ranks among entertainers with at least one active member. Return styles where demand exceeds active supply by at least 3, with style name, demand score, active supply score, gap, and booking revenue from entertainers whose strongest style is that style. Sort by largest gap, then lowest such revenue, then style name alphabetically.

style_name,demand_score,active_supply_score,demand_supply_gap,strongest_style_revenue
Rhythm and Blues,7,2,5,0
Show Tunes,5,1,4,4345
Jazz,8,4,4,5480
Standards,10,6,4,22630
40's Ballroom Music,3,0,3,0
Chamber Music,3,0,3,0
Modern Rock,3,0,3,0
Classical,3,0,3,2670
Contemporary,7,4,3,15070
Classic Rock & Roll,5,2,3,17150

## Query 2

Which five customers spent the most on bookings where the entertainer's strongest style was outside all of the customer's ranked musical preferences? Return customer last name, city, off-preference booking count, off-preference spend, and the number of distinct unpreferred strongest styles represented.

customer_last_name,city,off_preference_booking_count,off_preference_spend,distinct_unpreferred_strongest_styles
Hallmark,Auburn,7,25085,5
Berg,Tacoma,8,12970,5
Rosales,Bellevue,10,12770,4
Ehrlich,Kirkland,12,11955,7
Hartwig,Seattle,8,10795,6

## Query 3

Among same-state customer and entertainer pairs, find the five repeat off-preference relationships with the largest total spend, not the largest booking count. A repeat relationship has at least two bookings where the entertainer's strongest style is not one of the customer's ranked preferences. Return customer last name, entertainer stage name, state, customer's first-preference style, entertainer strongest style, booking count, and total spend.

customer_last_name,stage_name,state,first_preference_style,strongest_style,booking_count,off_preference_spend
Hallmark,Country Feeling,WA,Chamber Music,60's Music,2,15055
Berg,Country Feeling,WA,Variety,60's Music,3,6450
Hallmark,JV & the Deep Six,WA,Chamber Music,Classic Rock & Roll,2,5140
Waldal,Caroline Coie Cuartet,WA,60's Music,Contemporary,2,4000
Viescas,JV & the Deep Six,WA,Standards,Classic Rock & Roll,2,3880

## Query 4

Which entertainers are missing at least one contact channel and have an overnight-booking share above the overall booking average? Count only entertainers with at least one active member. Return stage name, active member count, total booking count, overnight booking count, overnight share rounded to 2 decimals, and overnight revenue.

stage_name,active_member_count,booking_count,overnight_booking_count,overnight_share,overnight_revenue
Caroline Coie Cuartet,3,11,3,0.27,2490

## Query 5

For months with at least 10 engagements, which three start months had the highest share of revenue from bookings where the customer's first-choice style matched the entertainer's strongest style? Return month label, booking count, total revenue, matching-style revenue, and matching-style revenue share rounded to 2 decimals.

month_label,booking_count,total_revenue,aligned_revenue,aligned_revenue_share
December 2017,17,17275,875,0.05
February 2018,20,21685,950,0.04
January 2018,28,43880,1240,0.03

## Query 6

Among agents paid below the median salary, which agents had a matching-style commission share above the average share computed across all agents before applying the salary filter? Matching-style commission comes from bookings where the customer's first-choice style matched the entertainer's strongest style. Return agent name, booking count, total commission, matching-style commission, and matching-style commission share rounded to 2 decimals.

agent_name,booking_count,total_commission,aligned_commission,aligned_commission_share
Caleb Viescas,8,372.58,33.25,0.09
Karen Smith,17,1022.73,48.13,0.05

## Query 7

For each agent, consider only bookings of entertainers with at least three active members and only months where that agent had at least two such bookings. What was each agent's best commission month? Return the top five agent-month rows overall with agent name, month label, booking count, revenue, and commission rounded to 2 decimals.

agent_name,month_label,booking_count,revenue,commission
John Kennedy,January 2018,6,20025,1201.50
Karen Smith,October 2017,4,7340,403.70
Marianne Wier,February 2018,4,6800,306.00
Carol Viescas,September 2017,2,5500,275.00
William Thompson,September 2017,3,4290,171.60

## Query 8

Which entertainers have at least two active members, a majority-female active roster, and total booking revenue above the entertainer median? Return stage name, active member count, female active member count, female active share rounded to 2 decimals, booking count, and total revenue.

stage_name,active_member_count,female_active_member_count,female_active_share,booking_count,total_revenue
Saturday Revue,3,3,1.00,9,11550

