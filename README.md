# データの取得方法

以下から取得して、 `data/` に配置してください

https://drive.google.com/drive/folders/1xp1-D_P6ea0bwE40y1zuVu4yOzRRML0L?usp=sharing

## review データ

`./data/review.json`

```sql
select
m.manual_id,
aor.overall_rating,
ar.check_in,
from `m2m_systems.air_official_reviews` aor
left join `m2m_systems.air_reservations` ar on aor.air_reservation_id = ar.id
left join `m2m_systems.air_listings` al on ar.air_listing_id = al.id
left join `m2m_core_prod.ota_room_type_relation` rtr on rtr.id_on_ota = al.airbnb_id_str
left join `m2m_core_prod.listing` l on rtr.core_room_type_id = l.room_type_id
left join `make_manual_id2.manual_id_table` m on m.room_id = l.id
where is_host = false and hidden = false and submitted = true
and m.manual_id is not null
```

# Build

```sh
$ duckdb < load.sql
```
