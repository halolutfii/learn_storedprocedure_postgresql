-- DROP FUNCTION public.GET_TOTAL_SUPPLEMENT_CURRENT_YEAR(int4, int4, int4);

CREATE OR REPLACE FUNCTION public.GET_TOTAL_SUPPLEMENT_CURRENT_YEAR(div_id integer, period integer, month_period integer)
 RETURNS TABLE(total_supplement double precision)
 LANGUAGE plpgsql
AS $function$
DECLARE
    org_name_ VARCHAR(50);
    org_id INT;
    total FLOAT;
BEGIN
    -- Mendapatkan nama organisasi
    SELECT org_name INTO org_name_
    FROM Mt_Organization_cc
    WHERE id = div_id;

    -- Kondisi untuk mengecek nama organisasi
    IF org_name_ = 'HC' THEN
        RETURN QUERY
        SELECT CAST(SUM(amount) AS FLOAT) AS total_supplement
        FROM Mt_Suplement_Budget
        WHERE deleted_at IS NULL
          AND CAST(a.year AS INTEGER) = period
          AND (CAST(a.month AS INTEGER)  BETWEEN 1 AND month_period OR month IS NULL)
          AND year = EXTRACT(YEAR FROM CURRENT_DATE);

    ELSIF org_name_ != 'HC GDH' THEN
        RETURN QUERY
        SELECT CAST(SUM(amount) AS FLOAT) AS total_supplement
        FROM Mt_Suplement_Budget a
        RIGHT OUTER JOIN Mt_CC b ON a.id_cc = b.id
        RIGHT OUTER JOIN Mt_OrgHierarchy_cc c ON c.id_org = b.id_div_dep
        RIGHT OUTER JOIN Mt_Organization_cc d ON d.id = c.id_org
        WHERE c.id_org_parent = div_id
          AND CAST(a.year AS INTEGER) = period
          AND (CAST(a.month AS INTEGER) BETWEEN 1 AND month_period OR a.month IS NULL)
          AND a.deleted_at IS NULL;

    ELSE
        RETURN QUERY
        SELECT CAST(SUM(amount) AS FLOAT) AS total_supplement
        FROM Mt_Suplement_Budget a
        RIGHT OUTER JOIN Mt_CC b ON a.id_cc = b.id
        RIGHT OUTER JOIN Mt_Organization_cc d ON d.id = b.id_div_dep
        WHERE d.id = div_id
          AND CAST(a.year AS INTEGER) = period
          AND (CAST(a.month AS INTEGER) BETWEEN 1 AND month_period OR a.month IS NULL)
          AND a.deleted_at IS NULL;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error: %', SQLERRM;
        RETURN;
END;
$function$
;