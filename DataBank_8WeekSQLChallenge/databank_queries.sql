/* A. Customer Nodes Exploration
1. How many unique nodes are there on the Data Bank system?
How many days on average are customers reallocated to a different node?
What is the median, 80th and 95th percentile for this same reallocation days metric for each region? */

select 
	count(distinct node_id) as unique_nodes
	from data_bank.customer_nodes
;

/* 2. What is the number of nodes per region? */

select regs.region_name,
count(distinct cns.node_id) as number_of_nodes
from data_bank.regions regs
inner join data_bank.customer_nodes cns
on regs.region_id=cns.region_id
group by regs.region_name
;

/* 3. How many customers are allocated to each region? */

select regs.region_name,
count(cns.customer_id) as customer_each_region
from data_bank.regions regs
inner join data_bank.customer_nodes cns
on regs.region_id=cns.region_id
group by regs.region_name
;

/* 4. How many days on average are customers reallocated to a different node? */

with node_days as(
select
customer_id,
node_id,
end_date - start_date as days_in_node
from data_bank.customer_nodes
where end_date != '9999-12-31'
group by customer_id, node_id, start_date, end_date
)
, total_node_days as (
select
customer_id,
node_id,
sum(days_in_node) as total_days_in_node
from node_days
group by customer_id, node_id
)
select round(avg(total_days_in_node)) as avg_node_reallocation_days
from total_node_days
;

/* 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region? */

with node_days as (
select 
cn.customer_id,
cn.region_id,
cn.node_id,
cn.end_date - cn.start_date as days_in_node
from data_bank.customer_nodes cn
where cn.end_date != '9999-12-31'
)
select 
r.region_name,
nd.region_id,
percentile_cont(0.5) within group (order by nd.days_in_node) as median_days,
percentile_cont(0.8) within group (order by nd.days_in_node) as p80_days,
percentile_cont(0.95) within group (order by nd.days_in_node) as p95_days
from node_days nd
inner join data_bank.regions r 
on nd.region_id = r.region_id
group by r.region_name, nd.region_id
order by nd.region_id
;

/* B. Customer Transactions
1. What is the unique count and total amount for each transaction type? */

select 
txn_type,
count(*) as unique_count,
sum(txn_amount) as total_amount
from data_bank.customer_transactions
group by txn_type
order by txn_type
;

/* 2. What is the average total historical deposit counts and amounts for all customers? */

select 
round(avg(txn_count)) as avg_deposit_count,
round(avg(txn_amount)) as avg_deposit_amount
from (
select
customer_id,
count(customer_id) as txn_count,
avg(txn_amount) as txn_amount
from data_bank.customer_transactions
where txn_type='deposit'
group by customer_id
) as deposit
;

/* 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month? */



with monthly_transactions as (
select
customer_id,
date_part('month', txn_date) as mth,
sum(case when txn_type='deposit' then 1 else 0 end) as deposit_count,
sum(case when txn_type='purchase' then 1 else 0 end) as purchase_count,
sum(case when txn_type='withdrawal' then 1 else 0 end) as withdrawal_count
from data_bank.customer_transactions
group by customer_id, date_part('month', txn_date)
)

select
mth,
count(distinct customer_id) as customer_count
from monthly_transactions
where deposit_count>1
and (purchase_count >= 1 or withdrawal_count >= 1)
group by mth
order by mth
;

/* 4. What is the closing balance for each customer at the end of the month? */
-- This question is too difficult, so i used Chat GPT to help me do some steps.
with monthly_balances_cte as (
select customer_id, 
date_trunc('month', txn_date) as closing_month, 
sum(case when txn_type in ('withdrawal', 'purchase') then -txn_amount else txn_amount end) as transaction_balance
from data_bank.customer_transactions
group by customer_id, closing_month
), 

monthend_series_cte as (
select distinct customer_id, 
generate_series(
  (select min(date_trunc('month', txn_date)) from data_bank.customer_transactions),
  (select max(date_trunc('month', txn_date)) from data_bank.customer_transactions),
  interval '1 month'
)::date as ending_month
from data_bank.customer_transactions
), 

monthly_changes_cte as (
select ms.customer_id, ms.ending_month, 
sum(coalesce(mb.transaction_balance, 0)) over (
partition by ms.customer_id 
order by ms.ending_month 
rows between unbounded preceding and current row) as ending_balance
from monthend_series_cte ms 
left join monthly_balances_cte mb 
on ms.ending_month = mb.closing_month 
and ms.customer_id = mb.customer_id
) 

select customer_id, ending_month, ending_balance 
from monthly_changes_cte 
order by ending_month, customer_id;





/* 5. What is the percentage of customers who increase their closing balance by more than 5%? */
-- This question is too difficult, so i used Chat GPT to help me do some steps.
with monthly_balances_cte as (
select customer_id, 
date_trunc('month', txn_date) as closing_month, 
sum(case when txn_type in ('withdrawal', 'purchase') then -txn_amount else txn_amount end) as transaction_balance
from data_bank.customer_transactions
group by customer_id, closing_month
), 

monthend_series_cte as (
select distinct customer_id, 
generate_series(
(select min(date_trunc('month', txn_date)) from data_bank.customer_transactions),
(select max(date_trunc('month', txn_date)) from data_bank.customer_transactions),
interval '1 month'
)::date as ending_month
from data_bank.customer_transactions
), 

monthly_changes_cte as (
select ms.customer_id, ms.ending_month, 
sum(coalesce(mb.transaction_balance, 0)) over (
partition by ms.customer_id 
order by ms.ending_month 
rows between unbounded preceding and current row) as ending_balance
from monthend_series_cte ms 
left join monthly_balances_cte mb 
on ms.ending_month = mb.closing_month 
and ms.customer_id = mb.customer_id
), 

balance_growth_cte as (
select customer_id, ending_month, ending_balance,
lag(ending_balance) over (partition by customer_id order by ending_month) as prev_balance,
case 
when lag(ending_balance) over (partition by customer_id order by ending_month) > 0
then round(100.0 * (ending_balance - lag(ending_balance) over (partition by customer_id order by ending_month)) 
/ lag(ending_balance) over (partition by customer_id order by ending_month), 2)
else null
end as balance_growth
from monthly_changes_cte
), 

customers_with_growth as (
select distinct customer_id 
from balance_growth_cte 
where balance_growth > 5.0
) 

select round(100.0 * count(distinct customers_with_growth.customer_id) 
/ nullif(count(distinct balance_growth_cte.customer_id), 0), 2) as increase_5_percentage
from balance_growth_cte
left join customers_with_growth 
on balance_growth_cte.customer_id = customers_with_growth.customer_id;





