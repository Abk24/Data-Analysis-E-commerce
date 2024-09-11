--1. List all unique cities where customers are located.
select distinct customer_city from customers


--2. Count the number of orders placed in 2017.
select COUNT(order_id) as total_orders from orders where DATEPART(year, order_purchase_timestamp) =2017


--3. Find the total sales per category.
select p.product_category,round(sum(py.payment_value),2) as revenue from order_items ot
join products p on p.product_id=ot.product_id
join payments py on py.order_id=ot.order_id 
group by p.product_category
order by sum(ot.price) desc


--4. Calculate the percentage of orders that were paid in installments.
select cast(sum(case 
when payment_installments >=1 
then 1 
else 0 
end) as decimal(10,2))/count(*) *100
from payments


--5. Count the number of customers from each state. 
select customer_state, COUNT(customer_id) as people from customers
group by customer_state
order by COUNT(customer_id) desc







