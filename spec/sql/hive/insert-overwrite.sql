-- Test INSERT OVERWRITE TABLE syntax
INSERT OVERWRITE TABLE `cdp_tmp_token_4028_segments`
select
  cast(key as string) as key,
  concat('[', concat_ws(',', collect_set(cast(value as string))), ']') as value
from cdp_tmp_token_4028_segments_all
where (length(cast(cast(key as string) as binary)) between 1 and 1023)
group by 1