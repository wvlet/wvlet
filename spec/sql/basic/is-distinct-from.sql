-- IS DISTINCT FROM tests
select 1 is distinct from 2;

select null is distinct from 1;

select null is distinct from null;

select 'hello' is not distinct from 'hello';

select 'hello' is not distinct from null;

select null is not distinct from null;

-- Test with CASE WHEN
select case when (status is distinct from expected_status) then 'Mismatch' else 'Match' end;

-- Test with complex expressions
select case when (t1.field is distinct from t2.field) then 'Different' end;