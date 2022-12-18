--Query que muestra la suma de todos los estados distintos de los clientes de 2017 y Peru
--Se ha puesto en orden según la ejecución seguida
--1 - Plan de ejecución sin índices
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states 
from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid);	

--2.1 - Eliminación y creación / creación de un índice del año de la fecha de order

DROP INDEX IF EXISTS iorders;
CREATE index iorders
ON orders (extract (year from orderdate));


--2.2 - Plan de ejecución índice del año
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states 
from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid);

--3.1 - Eliminación del índice de orders (si existe)
DROP INDEX IF EXISTS iorders;

--3.2 - Eliminación y creación / creación de un índice del país
DROP INDEX IF EXISTS icustomers;
CREATE index icustomers ON customers (country);

--3.3 - Plan de ejecución índice del país
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states 
from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid);


--4.1  - Eliminación y creación / creación de un índice del año de la fecha de order

DROP INDEX IF EXISTS iorders;
CREATE index iorders
ON orders (extract (year from orderdate));
   
--4.2 - Eliminación y creación / creación de un índice del país
DROP INDEX IF EXISTS icustomers;
CREATE index icustomers ON customers (country); 

--4.3 - Plan de ejecución índice del país
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states 
from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid);