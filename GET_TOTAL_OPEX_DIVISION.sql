-- DROP FUNCTION public."GET_TOTAL_OPEX_DIVISION"(int4, int4);

CREATE OR REPLACE FUNCTION public."GET_TOTAL_OPEX_DIVISION"(period integer, month_period integer)
 RETURNS TABLE(div_id integer, org_name text, total_opex double precision)
 LANGUAGE plpgsql
AS $function$
	DECLARE 
    var_r record;
	BEGIN
		-- Routine body goes here...	
		FOR var_r IN(
			SELECT 
		        d.id AS div_id,
		        d.org_name,
		        SUM(a.amount) AS total_opex
		    FROM Tx_Actual_Opex a
		    INNER JOIN Mt_CC b ON a.id_cc = b.id
		    INNER JOIN Mt_Organization_cc d ON d.id = b.id_div_dep
		    WHERE a.year = period
		      AND a.month BETWEEN 1 AND month_period
		      AND d.id = 4
		    GROUP BY d.id, d.org_name
		    
		     UNION
		    
		     SELECT 
		        d.id AS div_id,
		        d.org_name,
		        SUM(a.amount) AS total_opex
		    FROM Tx_Actual_Opex a
		    INNER JOIN Mt_CC b ON a.id_cc = b.id
		    INNER JOIN Mt_OrgHierarchy_cc c ON c.id_org = b.id_div_dep
		    INNER JOIN Mt_Organization_cc d ON d.id = c.id_org_parent AND d.org_type = 'Division'
		    WHERE a.year = period
		      AND a.month BETWEEN 1 AND month_period
		    GROUP BY d.id, d.org_name
		    ORDER BY total_opex ASC
		)
		LOOP
			div_id := var_r.div_id;
			org_name := var_r.org_name;
			total_opex := var_r.total_opex;
				RETURN NEXT;
				
		END LOOP;

	END$function$
;