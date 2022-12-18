--Para realizar el borrado, primero borraremos las referencias de orderdetail a orders que
--a su vez sean referencias de customers con dicho estado   
delete from orderdetail odt using orders o, customers c
where odt.orderid = o.orderid and o.customerid = c.customerid 
and c.state = %s;

--Después se borran las referencias de orders a los customers de dicho estado
delete from orders o using customers c
where o.customerid = c.customerid 
and c.state = %s;

--Por último se borran los customers de ese estado
delete from customers where state = %s;