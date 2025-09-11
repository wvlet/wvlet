-- Test the original GROUPING SETS issue
SELECT col1, col2, COUNT(*)
FROM table1 
GROUP BY GROUPING SETS (("from_contact"), ("from_addresses"), ("from_emails"), ("from_phones"), ("from_campaignmember"), ("from_asset"), ("from_crm_leadverteiler"), ("from_lead"), ("from_ati_events_interests"), ());