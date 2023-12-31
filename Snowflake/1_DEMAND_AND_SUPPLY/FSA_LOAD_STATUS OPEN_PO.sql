-- UPDATED SUPPLY
-- If the SUPPLY record has been seen before, but the HASH_VALUE is different, then set status to 'UPDATED'
UPDATE DEV.${vj_fsa_schema}.OPEN_PO t1
  SET FSA_LOAD_STATUS = 'UPDATED'
  FROM DEV.${vj_fsa_schema}.OPEN_PO_PREV_ASSIGNED t2
    WHERE t1.pk_id = t2.pk_id
        AND t1.hash_value != t2.hash_value;

-- UNCHANGED SUPPLY
-- If the SUPPLY record has been seen before, and the HASH_VALUE is the same, then set status to 'UNCHANGED'
UPDATE DEV.${vj_fsa_schema}.OPEN_PO t1
  SET FSA_LOAD_STATUS = 'UNCHANGED'
  FROM DEV.${vj_fsa_schema}.OPEN_PO_PREV_ASSIGNED t2
    WHERE t1.pk_id = t2.pk_id
        AND t1.hash_value = t2.hash_value;

-- NEW SUPPLY
-- If the SUPPLY record has not been processed before, then set status to 'NEW'
UPDATE DEV.${vj_fsa_schema}.OPEN_PO
SET FSA_LOAD_STATUS = 'NEW'
WHERE FSA_LOAD_STATUS IS NULL;

