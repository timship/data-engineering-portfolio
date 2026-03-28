CREATE OR REPLACE VIEW "marija-shkurat-wrn7887".item_brands_view AS
SELECT  
    brand,
    group_type,
    country,
    CAST(SUM(potential_revenue) AS FLOAT8) AS potential_revenue,
    CAST(SUM(total_revenue)     AS FLOAT8) AS total_revenue,
    CAST(COUNT(sku_id)          AS BIGINT) AS items_count
FROM "marija-shkurat-wrn7887".seller_items
GROUP BY brand, group_type, country;
