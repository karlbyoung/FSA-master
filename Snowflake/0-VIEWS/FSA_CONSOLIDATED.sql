SELECT ORDER_NUMBER
      ,A.NS_LINE_NUMBER
      ,A.TRANSACTION_ID
      ,A.LINE_ID
      /* 20230920 - KBY, RFS23-2696 If FSA_COMPLETE is 'F' then change FSA_OUTPUT_STATUS to signal update needed */
      ,CASE
        WHEN A.FSA_COMPLETE = 'F' AND A.FSA_OUTPUT_STATUS = 'UNCHANGED' 
          THEN 'UPDATED'
        ELSE A.FSA_OUTPUT_STATUS
       END                  AS FSA_OUTPUT_STATUS
      ,A.CREATE_DATE
      ,A.ROW_NO             AS "ROW_NO"
      /* 20230920 - KBY, RFS23-2696 If FSA_COMPLETE is 'F' then change CAPPING_DDA a bit to signal update needed */
      ,CASE
        WHEN A.FSA_COMPLETE = 'F' AND A.FSA_OUTPUT_STATUS = 'UNCHANGED' AND A.ORIG_CAP_DDA = A.CAPPING_DDA
          THEN DATEADD('day',-1,A.ORIG_CAP_DDA)
        ELSE A.ORIG_CAP_DDA
       END                  AS "ORIGINAL_CAPPING_DDA"
      ,A.FREDD              AS "FREDD"
      ,A.CAPPING_DDA        AS "CAPPING_DDA"
      ,A.SOURCE_TYPE
      ,A.LOCATION
      /* 20231109 - KBY, RFS23-3534 - Include FR Release Date */
      ,A.FR_RELEASE_DATE
FROM DEV.${vj_fsa_schema}.FSA A
WHERE SOURCE_TYPE = 'OpenSO'