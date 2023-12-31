# Flink_Project_Cquirrel
A realization based on Cquirrel Algorithm. Excecute query 3 on TPC-H

## Version Configuration

This project uses the following versions for key dependencies:

- Java: JDK 11
- Apache Flink: 1.18.0
- Maven: 3.8.1 (or the version you are using)
- Environment: WSL2

## Building the Project
use maven to get jar package, and run in WSL Flink cluster.

## Result
![3a8f360c6e8307b4c0a383651986f2c](https://github.com/LancelotGD/Flink_Project_Cquirrel/assets/35948261/52b832de-125c-4945-ad1d-871091a8ad97)

## Algorithm Reference

Demo:https://cse.hkust.edu.hk/~yike/Cquirrel.pdf

Full Paper and Algorithm details:https://cse.hkust.edu.hk/~yike/sigmod20.pdf

## SQL query 
```sql
select
    l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,o_orderdate, o_shippriority
from
    customer c, orders o, lineitem l
where
        c_mktsegment = 'BUILDING'
  and c_custkey=o_custkey
  and l_orderkey=o_orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15'
  and l_receiptdate > l_commitdate
group by
    l_orderkey, o_orderdate, o_shippriority;
```
