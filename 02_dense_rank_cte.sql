with t1 as (SELECT *, dense_rank() OVER(PARTITION BY user_id ORDER BY time) dr
FROM   user_actions
WHERE  order_id not in (SELECT order_id
FROM   user_actions
WHERE  action = 'cancel_order')
), 
                                    
t2 as (SELECT *,
to_char(date_trunc('day', time), 'YYYY-MM-DD') as date,
case when dr = 1 then 'Первый'
else 'Повторный' end as order_type
FROM   t1),

t3 AS
(SELECT date,
order_type,
count(order_id) orders_count
FROM   t2
GROUP BY 1, 2
ORDER BY 1, 2)

SELECT *, ROUND(orders_count/SUM(orders_count) OVER(PARTITION BY date), 2) orders_share FROM t3
