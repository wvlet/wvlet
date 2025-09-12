-- Test IP address literals
-- These are Trino-specific IP address literals

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