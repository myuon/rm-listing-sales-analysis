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
    sum(total) / sum(try_cast(total_days_in_the_month as integer)) as total_adr,
    sum(minpaku_total) / sum(try_cast(minpaku_days_in_the_month as integer)) as minpaku_adr,
    sum(monthly_total) / sum(try_cast(monthly_days_in_the_month as integer)) as monthly_adr,
    sum(total) as total,
    sum(minpaku_total) as minpaku_total,
    sum(monthly_total) as monthly_total,
from read_json('./data/sales.json') group by listing_id, substr(try_cast(month as varchar), 0, 5);

copy (
    select
        listings.*,
        yearly_sales.minpaku_total,
        yearly_sales.monthly_total,
        yearly_sales.total,
        yearly_sales.minpaku_adr,
        yearly_sales.monthly_adr,
        yearly_sales.total_adr,
    from listings
    join yearly_sales
        on listings.manual_id = yearly_sales.manual_id
    where yearly_sales.year = '2024'
) to './data/listings_2024_sales.json';
