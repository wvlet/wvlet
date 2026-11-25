-- SQL: union all and subqueries
select r_name as name, 'region' as kind from region
union all
select n_name as name, 'nation' as kind from nation where n_regionkey in (select r_regionkey from region where r_name = 'EUROPE')
order by kind, name
