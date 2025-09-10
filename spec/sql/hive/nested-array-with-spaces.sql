-- Test nested array type with spaces to avoid >> tokenization issue
SELECT CAST(array(array(1)) AS array<array<int> >);