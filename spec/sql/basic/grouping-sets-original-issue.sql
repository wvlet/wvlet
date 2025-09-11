-- Test the original GROUPING SETS issue with data
WITH contact_data AS (
  SELECT * FROM VALUES
    ('contact1', 'addr1', 'email1', 'phone1', 'member1', 'asset1', 'lead1', 'lead1', 'event1'),
    ('contact2', 'addr2', 'email2', 'phone2', 'member2', 'asset2', 'lead2', 'lead2', 'event2'),
    ('contact3', 'addr3', 'email3', 'phone3', 'member3', 'asset3', 'lead3', 'lead3', 'event3')
  AS t(from_contact, from_addresses, from_emails, from_phones, from_campaignmember, from_asset, from_crm_leadverteiler, from_lead, from_ati_events_interests)
)
SELECT from_contact, from_addresses, COUNT(*) as count
FROM contact_data
GROUP BY GROUPING SETS ((from_contact), (from_addresses), (from_emails), (from_phones), (from_campaignmember), (from_asset), (from_crm_leadverteiler), (from_lead), (from_ati_events_interests), ());