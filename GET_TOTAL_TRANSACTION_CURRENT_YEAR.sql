-- DROP FUNCTION public."GET_TOTAL_TRANSACTION_CURRENT_YEAR"();

CREATE OR REPLACE FUNCTION public."GET_TOTAL_TRANSACTION_CURRENT_YEAR"()
 RETURNS TABLE(total_transaction double precision)
 LANGUAGE plpgsql
AS $function$
DECLARE 
    var_r RECORD;
BEGIN
    -- Routine body goes here...    
    FOR var_r IN(
   	 	SELECT
		CAST(count(id) as float) total_transaction
		FROM tx_actual_opex
		where deleted_at is null
		AND YEAR = EXTRACT(YEAR FROM CURRENT_DATE)
    )        
    LOOP
        total_transaction := var_r.total_transaction;  
        RETURN NEXT;
    END LOOP;

    RETURN;
END $function$
;