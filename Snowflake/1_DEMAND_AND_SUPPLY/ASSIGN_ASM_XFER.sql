/* 20240516 - KBY, RFS-5213 Assign assembly orders with any Assembly Transfer Orders before checking inventory */
CALL DEV.${vj_fsa_schema}.ASSIGN_ASM_XFER();
