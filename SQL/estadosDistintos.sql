Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.city = 'action'
group by c.customerid, cyear
order by c.customerid;
