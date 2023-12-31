MERGE INTO DEV.${vj_fsa_schema}.OPEN_PO_PREV_ASSIGNED t1
    USING DEV.${vj_fsa_schema}.OPEN_PO t2
        ON t1.pk_id = t2.pk_id
    WHEN matched AND t1.HASH_VALUE != t2.HASH_VALUE 
    	THEN UPDATE SET t1.HASH_VALUE = t2.HASH_VALUE, t1.LAST_MODIFIED = t2.INSERT_DATE
    WHEN NOT matched THEN INSERT VALUES
    (t2.PK_ID,
      t2.HASH_VALUE,
      t2.INSERT_DATE,
      t2.INSERT_DATE -- LAST_MODIFIED
    );
