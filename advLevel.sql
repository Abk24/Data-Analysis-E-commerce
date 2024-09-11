--1. Calculate the moving average of order values for each customer 
--over their order history.
select o.customer_id , 
avg(py.payment_value) 
over(partition by o.customer_id order by order_purchase_timestamp
rows between 2 preceding and current row
) as running_avg
from orders o 
join payments py on o.order_id=py.order_id


--2. Calculate the cumulative sales per month for each year.

with cte as(
select DATEPART(year,o.order_purchase_timestamp) as years,
DATEPART(month,o.order_purchase_timestamp) as months,
sum(py.payment_value) as revenue
from payments py
join orders o on o.order_id=py.order_id
group by DATEPART(year,o.order_purchase_timestamp),
DATEPART(month,o.order_purchase_timestamp)
)
select *, 
avg(round(revenue,2)) over(order by years, months) as cumulative_sum 
from cte


--3. Calculate the year-over-year growth rate of total sales.
with cte as(
select DATEPART(year,o.order_purchase_timestamp) as years, 
round(sum(py.payment_value),2) as revenue,
LAG(round(sum(py.payment_value),2)) over(order by DATEPART(year,o.order_purchase_timestamp)) as previous_year_revenue
from payments py
join orders o on o.order_id= py.order_id
group by DATEPART(year,o.order_purchase_timestamp)
)
select years, concat(round(((revenue-previous_year_revenue)/previous_year_revenue )*100,0),  ' %') as 'growth_in_%'  from cte 



--4. Calculate the retention rate of customers, 
--defined as the percentage of customers who make another 
--purchase within 6 months of their first purchase.
with cte as(
select customer_id, min(order_purchase_timestamp) as first_order from orders
group by customer_id
), cte2 as(
select cte.customer_id, count(distinct o.order_purchase_timestamp) as ords from cte 
join orders o on o.customer_id=cte.customer_id
where o.order_purchase_timestamp > cte.first_order and 
o.order_purchase_timestamp<= DATEADD(MONTH, 6, cte.first_order)
group by cte.customer_id
)
select  count(distinct cte.customer_id)/count( distinct cte2.customer_id) *100 
from cte left join cte2 on cte.customer_id=cte2.customer_id


--5. Identify the top 3 customers who spent most of the money each year
with cte as(
select year(o.order_purchase_timestamp) year, o.customer_id, round(sum(py.payment_value),2) spending,
DENSE_RANK() over(partition by year(o.order_purchase_timestamp) order by sum(py.payment_value) desc) as rnk
from orders o 
join payments py on py.order_id=o.order_id
group by year(o.order_purchase_timestamp),o.customer_id
)
select * from cte where rnk<=3



