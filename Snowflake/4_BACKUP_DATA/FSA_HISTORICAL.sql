INSERT INTO DEV.${FSA_CURRENT_SCHEMA}.FSA_HISTORICAL
SELECT *, CURRENT_TIMESTAMP()
FROM DEV.${FSA_CURRENT_SCHEMA}.FSA;