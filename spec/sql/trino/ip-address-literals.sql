-- Test IP address literals
-- These are Trino-specific IP address literals
-- parse-only: true

-- Basic IPv4 addresses
SELECT IPADDRESS '192.168.1.1' as ip1;
SELECT IPADDRESS '10.0.0.1' as ip2;
SELECT IPADDRESS '255.255.255.255' as broadcast;

-- IPv6 addresses
SELECT IPADDRESS '::1' as localhost_ipv6;
SELECT IPADDRESS '2001:db8::1' as ipv6_example;

-- IP address in WHERE clause
SELECT * FROM VALUES (IPADDRESS '192.168.1.1') as t(ip) WHERE ip = IPADDRESS '192.168.1.1';

-- IP address as column name (should be treated as identifier)
SELECT ipaddress FROM VALUES ('test') as t(ipaddress);

-- Case-insensitivity
SELECT ipaddress '127.0.0.1' as ip_lower;

-- IPADDRESS as a function call (should be treated as a function)
SELECT IPADDRESS('127.0.0.1') as ip_func;

-- IPADDRESS with double-quoted string literal
SELECT IPADDRESS "1.2.3.4" as ip_double_quoted;