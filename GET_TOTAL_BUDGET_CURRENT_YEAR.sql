-- DROP FUNCTION public."GET_TOTAL_BUDGET_CURRENT_YEAR"();

CREATE OR REPLACE FUNCTION public."GET_TOTAL_BUDGET_CURRENT_YEAR"()
 RETURNS TABLE(total_budget double precision)
 LANGUAGE plpgsql
AS $function$
DECLARE 
    var_r RECORD;
BEGIN
    -- Routine body goes here...    
    FOR var_r IN
        SELECT CAST(SUM(amount) AS FLOAT) AS total_budget
        FROM Mt_Budget
        WHERE deleted_at IS NULL
        AND EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM CURRENT_DATE)
    LOOP
        total_budget := var_r.total_budget;  
        RETURN NEXT;
    END LOOP;

    RETURN;
END $function$
;