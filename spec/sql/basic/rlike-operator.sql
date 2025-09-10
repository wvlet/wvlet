-- Test RLIKE operator support

-- Basic RLIKE usage
select * from users where name rlike '^[A-Z].*';

-- RLIKE with complex regex pattern
select * from logs where message rlike '(error|warning|fatal).*[0-9]+';

-- NOT RLIKE
select * from products where description not rlike '^test.*';

-- RLIKE with special characters in pattern
select * from data where field rlike '\\d{3}-\\d{2}-\\d{4}';

-- Hive-style regex with Googlebot pattern (from the original error)
select * from page_views
where td_browser is null or not td_browser rlike '^(?:Googlebot(?:-.*)?|BingPreview|bingbot|YandexBot|PingdomBot)';

-- Multiple RLIKE conditions
select * from events
where event_type rlike '^user_.*'
  and event_data rlike '.*success.*'
  and not event_source rlike '^internal.*';

-- RLIKE with column references
select a.* from table_a a
join table_b b on a.id = b.id
where a.pattern_col rlike b.regex_col;