INSERT INTO DEV.${FSA_CURRENT_SCHEMA}.DEMAND_PREV_ASSIGNED_HISTORICAL
SELECT 	*, 
		TRUE USED_IN_PROCESS, 
        CURRENT_TIMESTAMP() FSA_INSERT_DATE
FROM DEV.${FSA_CURRENT_SCHEMA}.DEMAND_PREV_ASSIGNED;