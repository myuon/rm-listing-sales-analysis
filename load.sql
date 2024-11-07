CREATE TABLE listings AS
SELECT
    json->>'manual_id' AS manual_id,
    json->>'listing_name' AS listing_name,
    json->>'room_type_id' AS room_type_id,
    json->>'name' AS owner_name,
    json->>'stay_operation_type' AS stay_operation_type,
    json->>'prefecture_name' AS prefecture_name,
    json->>'city_name' AS city_name,
    json->>'floor_plan' AS floor_plan,
    try_cast(json->>'sqm' as double) AS sqm,
    try_cast(json->>'number_of_capacity' as integer) AS number_of_capacity,
    json->>'has_elevator' AS has_elevator,
    json->>'has_auto_lock' AS has_auto_lock,
    json->>'first_line' AS first_line,
    json->>'first_station' AS first_station,
    json->>'first_walk_min' AS first_walk_min,
    try_cast(json->>'location_floor' as integer) AS location_floor,
    try_cast(json->>'built_year' as integer) AS built_year,
    json->>'tag' AS tag,
    try_cast(json->>'number_of_s_beds' as integer) AS number_of_s_beds,
    try_cast(json->>'number_of_sd_beds' as integer) AS number_of_sd_beds,
    try_cast(json->>'numboer_of_d_beds' as integer) AS number_of_d_beds,
    try_cast(json->>'number_of_q_beds' as integer) AS number_of_q_beds,
    try_cast(json->>'number_of_k_beds' as integer) AS number_of_k_beds,
    try_cast(json->>'number_of_futons' as integer) AS number_of_futons,
    try_cast(json->>'number_of_sofa_beds' as integer) AS number_of_sofa_beds,
FROM read_json('./data/listings.json');

create table yearly_sales as
select
    listing_id,
    substr(try_cast(month as varchar), 0, 5) as year,
    any_value(name) as name,
    any_value(manual_id) as manual_id,
    sum(total) as total,
    sum(minpaku_total) as minpaku_total,
    sum(monthly_total) as monthly_total,
    sum(try_cast(total_days_in_the_month as integer)) as total_days,
    sum(try_cast(minpaku_days_in_the_month as integer)) as minpaku_total_days,
    sum(try_cast(monthly_days_in_the_month as integer)) as monthly_total_days,
from read_json('./data/sales.json')
group by listing_id, substr(try_cast(month as varchar), 0, 5)
order by listing_id, substr(try_cast(month as varchar), 0, 5);

copy yearly_sales to './data/yearly_sales.json';

copy (
    select
        listings.manual_id,
        ANY_VALUE(listings.listing_name) as listing_name,
        ANY_VALUE(listings.room_type_id) as room_type_id,
        ANY_VALUE(listings.owner_name) as owner_name,
        ANY_VALUE(listings.stay_operation_type) as stay_operation_type,
        ANY_VALUE(listings.prefecture_name) as prefecture_name,
        ANY_VALUE(listings.city_name) as city_name,
        ANY_VALUE(listings.floor_plan) as floor_plan,
        ANY_VALUE(listings.sqm) as sqm,
        ANY_VALUE(listings.number_of_capacity) as number_of_capacity,
        ANY_VALUE(listings.has_elevator) as has_elevator,
        ANY_VALUE(listings.has_auto_lock) as has_auto_lock,
        ANY_VALUE(listings.first_line) as first_line,
        ANY_VALUE(listings.first_station) as first_station,
        ANY_VALUE(listings.first_walk_min) as first_walk_min,
        ANY_VALUE(listings.location_floor) as location_floor,
        ANY_VALUE(listings.built_year) as built_year,
        ANY_VALUE(listings.tag) as tag,
        ANY_VALUE(listings.number_of_s_beds) as number_of_s_beds,
        ANY_VALUE(listings.number_of_sd_beds) as number_of_sd_beds,
        ANY_VALUE(listings.number_of_d_beds) as number_of_d_beds,
        ANY_VALUE(listings.number_of_q_beds) as number_of_q_beds,
        ANY_VALUE(listings.number_of_k_beds) as number_of_k_beds,
        ANY_VALUE(listings.number_of_futons) as number_of_futons,
        ANY_VALUE(listings.number_of_sofa_beds) as number_of_sofa_beds,
        sum(yearly_sales.total) / sum(yearly_sales.total_days) as total_adr,
        sum(yearly_sales.minpaku_total) / sum(yearly_sales.minpaku_total_days) as minpaku_adr,
        sum(yearly_sales.monthly_total) / sum(yearly_sales.monthly_total_days) as monthly_adr,
    from listings
    join yearly_sales
        on listings.manual_id = yearly_sales.manual_id
    where yearly_sales.total_days > 0
    and yearly_sales.minpaku_total_days > 0
    and yearly_sales.monthly_total_days > 0
    and yearly_sales.total > 0
    and yearly_sales.year IN ('2023', '2024', '2025')
    and yearly_sales.minpaku_total > 0
    and yearly_sales.monthly_total > 0
    group by listings.manual_id
) to './data/listings_2024_sales.json';

create table reviews as
select * from read_json('./data/reviews.json');

copy (
    select
        listings.manual_id,
        ANY_VALUE(listings.listing_name) as listing_name,
        ANY_VALUE(listings.room_type_id) as room_type_id,
        ANY_VALUE(listings.owner_name) as owner_name,
        ANY_VALUE(listings.stay_operation_type) as stay_operation_type,
        ANY_VALUE(listings.prefecture_name) as prefecture_name,
        ANY_VALUE(listings.city_name) as city_name,
        ANY_VALUE(listings.floor_plan) as floor_plan,
        ANY_VALUE(listings.sqm) as sqm,
        ANY_VALUE(listings.number_of_capacity) as number_of_capacity,
        ANY_VALUE(listings.has_elevator) as has_elevator,
        ANY_VALUE(listings.has_auto_lock) as has_auto_lock,
        ANY_VALUE(listings.first_line) as first_line,
        ANY_VALUE(listings.first_station) as first_station,
        ANY_VALUE(listings.first_walk_min) as first_walk_min,
        ANY_VALUE(listings.location_floor) as location_floor,
        ANY_VALUE(listings.built_year) as built_year,
        ANY_VALUE(listings.tag) as tag,
        ANY_VALUE(listings.number_of_s_beds) as number_of_s_beds,
        ANY_VALUE(listings.number_of_sd_beds) as number_of_sd_beds,
        ANY_VALUE(listings.number_of_d_beds) as number_of_d_beds,
        ANY_VALUE(listings.number_of_q_beds) as number_of_q_beds,
        ANY_VALUE(listings.number_of_k_beds) as number_of_k_beds,
        ANY_VALUE(listings.number_of_futons) as number_of_futons,
        ANY_VALUE(listings.number_of_sofa_beds) as number_of_sofa_beds,
        avg(try_cast(reviews.overall_rating as integer)) as overall_rating,
    from listings
    join reviews
        on listings.manual_id = reviews.manual_id
    where try_cast(reviews.check_in as varchar) >= '2023' and try_cast(reviews.check_in as varchar) <= '2025'
    group by listings.manual_id
) to './data/listings_2024_reviews.json';
