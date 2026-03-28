CREATE OR REPLACE VIEW "marija-shkurat-wrn7887".unreliable_sellers_view AS
SELECT  
    seller,
    SUM(availability_items_count) AS total_overload_items_count,
    CASE WHEN (AVG(days_on_sell) > 100 AND SUM(availability_items_count) > SUM(ordered_items_count)) 
         THEN TRUE 
         ELSE FALSE 
         END AS is_unreliable
FROM "marija-shkurat-wrn7887".seller_items
GROUP BY seller;
