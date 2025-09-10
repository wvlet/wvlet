-- Test ESCAPE clause support for LIKE expressions

-- Basic ESCAPE clause with backslash
SELECT *
FROM (VALUES ('field_name'), ('field\_name'), ('field%name')) AS t(name)
WHERE name LIKE 'field\_name' ESCAPE '\';

-- ESCAPE clause with different escape character
SELECT *
FROM (VALUES ('field_name'), ('field#_name'), ('field%name')) AS t(name)
WHERE name LIKE 'field#_name' ESCAPE '#';

-- NOT LIKE with ESCAPE clause
SELECT *
FROM (VALUES ('field_name'), ('field\_name'), ('field%name')) AS t(name)
WHERE name NOT LIKE 'field\_name' ESCAPE '\';

-- Complex example from the original error
SELECT
  f_67199
, f_eb709
, f_c7efd
FROM (VALUES 
  (1, 'eur_consents_source', 'gbr_krux_consents_stdx'),
  (2, 'eur\_consents\_source', 'gbr\_krux\_consents\_stdx'),
  (3, 'other', 'other')
) AS t(f_67199, f_eb709, f_c7efd)
WHERE ((f_eb709 LIKE 'eur\_consents\_source' ESCAPE '\') AND (f_c7efd LIKE 'gbr\_krux\_consents\_stdx' ESCAPE '\'))
ORDER BY f_67199 ASC, f_eb709 ASC, f_c7efd ASC;

-- ESCAPE with special characters
SELECT *
FROM (VALUES ('100%'), ('100!%'), ('100%%')) AS t(value)
WHERE value LIKE '100!%' ESCAPE '!';

-- Multiple LIKE conditions with different ESCAPE characters
SELECT *
FROM (VALUES ('a_b'), ('a%b'), ('a#b'), ('a\_b')) AS t(col)
WHERE col LIKE 'a\_b' ESCAPE '\' OR col LIKE 'a#%b' ESCAPE '#';