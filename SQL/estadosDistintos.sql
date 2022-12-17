--Query que muestra la suma de todos los estados distintos de los clientes de 2017 y la ciudad action
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states from customers c
join orders o on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid); 


--indice customersid (no hay un cambio significativo)
BEGIN
    IF EXISTS (SELECT icustomers FROM customers) THEN
        DROP index icustomers; 
    ELSE
        CREATE INDEX icustomers
        ON customers (customerid);
    END IF;
end;


BEGIN
    IF EXISTS (SELECT iorders FROM orders) THEN
        drop index iorders; 
    ELSE
        CREATE index iorders
        ON orders (extract (year from orderdate));
    END IF;
end;


BEGIN
    IF EXISTS (SELECT icustomers FROM customers) THEN
        DROP INDEX icustomers; 
    ELSE
        CREATE index icustomers ON customers (country);
    END IF;
end;


--Query con el orden de accesi cambiado
explain analyze (Select c.customerid, extract (year from o.orderdate) as cyear, count(distinct(c.state)) as diferent_states
from orders o 
join  customers c on o.customerid = c.customerid 
where extract (year from o.orderdate) = 2017 and c.country = 'Peru'
group by c.customerid, cyear
order by c.customerid); 
