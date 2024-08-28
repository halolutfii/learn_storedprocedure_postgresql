CREATE OR REPLACE FUNCTION get_total_opex_division_monthly(
    div_id INT,
    period INT,
    month_period INT
) RETURNS TABLE(
    id TEXT,
    org_name TEXT,
    year TEXT,
    month TEXT,
    total_opex TEXT
) LANGUAGE plpgsql
AS $$
DECLARE
    var_r RECORD;
BEGIN
    IF div_id = 4 THEN
        FOR var_r IN
            SELECT 
                d.id AS t_id,
                d.org_name AS t_org_name,
                a.year AS t_year,
                TO_CHAR(TO_DATE(a.month::TEXT, 'MM'), 'Mon') AS t_month,
                ROUND(SUM(a.amount) / 1000000, 1) AS t_total_opex
            FROM Tx_Actual_Opex a
            INNER JOIN Mt_CC b ON a.id_cc = b.id
            INNER JOIN Mt_Organization_cc d ON d.id = b.id_div_dep
            WHERE d.id = div_id
              AND a.year = period
              AND a.month BETWEEN 1 AND month_period
            GROUP BY d.id, d.org_name, a.year, a.month
            ORDER BY a.month
        LOOP
			id := var_r.t_id;
			org_name := var_r.t_org_name;
			year := var_r.t_year;
			month := var_r.t_month;
			total_opex := var_r.t_total_opex;
			RETURN NEXT;
        END LOOP;
    ELSE
        FOR var_r IN
            SELECT 
                d.id AS t_id,
                d.org_name AS t_org_name,
                a.year AS t_year,
                TO_CHAR(TO_DATE(a.month::TEXT, 'MM'), 'Mon') AS t_month,
                ROUND(SUM(a.amount) / 1000000, 1) AS t_total_opex
            FROM Tx_Actual_Opex a
            INNER JOIN Mt_CC b ON a.id_cc = b.id
            INNER JOIN Mt_OrgHierarchy_cc c ON c.id_org = b.id_div_dep
            INNER JOIN Mt_Organization_cc d ON d.id = c.id_org_parent AND d.org_type = 'Division'
            WHERE d.id = div_id
              AND a.year = period
              AND a.month BETWEEN 1 AND month_period
            GROUP BY d.id, d.org_name, a.year, a.month
            ORDER BY a.month
        LOOP
			id := var_r.t_id;
			org_name := var_r.t_org_name;
			year := var_r.t_year;
			month := var_r.t_month;
			total_opex := var_r.t_total_opex;
			RETURN NEXT;
        END LOOP;
    END IF;
END;
$$;