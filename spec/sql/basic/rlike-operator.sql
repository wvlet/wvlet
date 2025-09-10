-- Test RLIKE operator support

-- Basic RLIKE usage with VALUES
select * from (values ('Alice'), ('Bob'), ('Charlie')) as t(name)
where name rlike '^[A-Z].*';

-- RLIKE with complex regex pattern
select * from (
  values 
    ('error: connection failed at line 123'),
    ('warning: low memory at 456'),
    ('info: server started'),
    ('fatal: disk full 789')
) as logs(message)
where message rlike '(error|warning|fatal).*[0-9]+';

-- NOT RLIKE
select * from (
  values ('test_product'), ('main_product'), ('beta_product')
) as products(description)
where description not rlike '^test.*';

-- RLIKE with special characters in pattern (SSN pattern)
select * from (
  values ('123-45-6789'), ('999-88-7777'), ('invalid-ssn')
) as data(field)
where field rlike '\\d{3}-\\d{2}-\\d{4}';

-- Hive-style regex with Googlebot pattern
select * from (
  values 
    ('Googlebot/2.1'),
    ('Mozilla/5.0'),
    ('bingbot/2.0'),
    ('YandexBot/3.0')
) as browsers(user_agent)
where user_agent not rlike '^(?:Googlebot(?:-.*)?|BingPreview|bingbot|YandexBot|PingdomBot)';

-- Multiple RLIKE conditions
select * from (
  values 
    ('user_login', 'success', 'external'),
    ('user_logout', 'success', 'internal'),
    ('system_check', 'failure', 'internal')
) as events(event_type, event_data, event_source)
where event_type rlike '^user_.*'
  and event_data rlike '.*success.*'
  and not event_source rlike '^internal.*';

-- RLIKE with join
select a.* from 
  (values ('pattern1', '^test.*'), ('pattern2', '^prod.*')) as a(id, pattern_col),
  (values ('test_data', '^test.*'), ('prod_data', '^prod.*')) as b(data, regex_col)
where a.pattern_col = b.regex_col;