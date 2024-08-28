CREATE OR REPLACE FUNCTION get_total_detail_department_level2(
    year_start INT,
    month_start INT,
    year_end INT,
    month_end INT,
    id_parent INT
)
RETURNS TABLE (
    level TEXT,
    gl_name TEXT,
    expenses_name TEXT,
    budget_category_name TEXT,
    sub_budget_category_name TEXT,
    detail_budget_category_name TEXT,
    actual_before TEXT,
    budget_amount TEXT,
    supplement_amount TEXT,
    carryforward_amount TEXT,
    total_budget TEXT,
    actual_amount TEXT,
    actual_percent TEXT,
    outstanding_amount TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE 
    var_r record;
BEGIN
	DROP TABLE IF EXISTS TOrg;
	DROP TABLE IF EXISTS TOrgCC;
	
    CREATE TEMP TABLE TOrg (id INT);
    CREATE TEMP TABLE TOrgCC (id INT);
	
	WITH RECURSIVE descendant AS (
	    SELECT
	        id_org,
	        id_org_parent,
	        0 AS t_level
	    FROM
	        Mt_OrgHierarchy_cc
	    WHERE
	        id_org_parent = id_parent
	
	    UNION ALL
	
	    SELECT
	        ft.id_org,
	        ft.id_org_parent,
	        d.t_level + 1
	    FROM
	        Mt_OrgHierarchy_cc ft
	    JOIN
	        descendant d ON ft.id_org_parent = d.id_org
	)
	
	INSERT INTO TOrg
	SELECT
	    d.id_org AS descendant_id
	FROM
	    descendant d
	LEFT JOIN
	    Mt_OrgHierarchy_cc a ON d.id_org_parent = a.id_org
	ORDER BY
	    t_level;
	
	INSERT INTO TOrgCC
	SELECT
	    id
	FROM
	    Mt_CC
	WHERE
	    id_div_dep IN (SELECT id FROM TOrg)
	    OR id_div_dep = id_parent;
	
	FOR var_r IN (
		SELECT
		    t_level,
		    t_gl_name,
		    t_expenses_name,
		    t_budget_category_name,
		    t_sub_budget_category_name,
		    t_detail_budget_category_name,
		    t_actual_before,
		    t_budget_amount,
		    t_supplement_amount,
		    t_carryforward_amount,
		    COALESCE(t_total_budget, 0), 1000000 AS t_total_budget,
		    COALESCE(t_actual_amount, 0), 1000000 AS t_actual_amount,
			((COALESCE(t_actual_amount, 0) / t_total_budget) * 100) AS t_actual_percent,
			t_outstanding_amount
		FROM (
		    SELECT
		        t_level,
		    	t_gl_name,
		        t_expenses_name,
				budget_category_id,
		        t_budget_category_name,
		        t_sub_budget_category_name,
		        t_detail_budget_category_name,
			    SUM(t_actual_before) AS t_actual_before,
			    SUM(t_budget_amount) AS t_budget_amount,
			    SUM(t_supplement_amount) AS t_supplement_amount,
			    SUM(t_carryforward_amount) AS t_carryforward_amount,
			    SUM(t_budget_amount) + SUM(t_supplement_amount) + SUM(t_carryforward_amount) AS t_total_budget,
			    SUM(t_actual_amount) AS t_actual_amount,
			    SUM(t_outstanding_amount) AS t_outstanding_amount
		    FROM (
			    SELECT 
			        '2' AS t_level,
			        t_budget_category_name AS t_gl_name,
			        t_expenses_name,
					budget_category_id,
			        t_budget_category_name,
			        NULL AS t_sub_budget_category_name,
			        NULL AS t_detail_budget_category_name,
			        COALESCE(SUM(actualbefore), 0) AS t_actual_before,
			        COALESCE(SUM(budgetamount), 0) AS t_budget_amount,
			        COALESCE(SUM(supplementamount), 0) AS t_supplement_amount,
			        COALESCE(SUM(carryforwardamount), 0) AS t_carryforward_amount,
			        COALESCE(SUM(actualamount), 0) AS t_actual_amount,
			        (SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
			    FROM (
			        SELECT 
			            GL.id,
			            GL.gl_code,
			            GL.gl_desc,
			            GL.nature_coa,
			            Mt_Expenses.name AS t_expenses_name,
						Mt_Budget_Category.id AS budget_category_id,
						Mt_Budget_Category.name AS t_budget_category_name,
						Mt_Sub_Budget_Category.name AS t_sub_budget_category_name,
						Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
						CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
						END AS budgetamount,
						CASE
							WHEN SP.SumAmount IS NULL THEN '0'
							ELSE SP.SumAmount
						END AS supplementamount,
						CASE
							WHEN CF.SumAmount IS NULL THEN '0'
							ELSE CF.SumAmount
						END AS carryforwardamount,
						CASE
							WHEN ACT.SumAmount IS NULL THEN '0'
							ELSE ACT.SumAmount
						END AS actualamount,
						CASE
							WHEN ACTB.SumBefore IS NULL THEN '0'
							ELSE ACTB.SumBefore
						END AS actualbefore
			        FROM
			        	Mt_GL AS GL
			        INNER JOIN
			            Mt_Expenses ON GL.expenses = Mt_Expenses.id
			        INNER JOIN
			    		Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
			    	INNER JOIN
			    		Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN
						Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
			        LEFT JOIN (
			            SELECT id_gl,
			            SUM(amount) AS SumAmount
			            FROM
			            	Mt_Budget
			            WHERE
			            	CAST(year AS INTEGER) BETWEEN year_start AND year_end 
			            	AND id_cc IN (SELECT id FROM TOrgCC) 
			            	AND deleted_at IS NULL
			            GROUP BY
			            	id_gl
			        ) AS B ON B.id_gl = GL.id
			        LEFT JOIN (
			            SELECT
			            	id_gl,
			            	SUM(amount) AS SumAmount
			            FROM
			            	Mt_Suplement_Budget
			            WHERE
			            	CAST(year AS INTEGER) = year_end 
			            	AND id_cc IN (SELECT id FROM TOrgCC) 
			            	AND deleted_at IS NULL
			            GROUP BY
			            	id_gl
			        ) AS SP ON SP.id_gl = GL.id
			        LEFT JOIN (
			            SELECT
			            	id_gl,
			            	SUM(amount) AS SumAmount
			            FROM	
			            	Mt_Carryforward_Budget
			            WHERE
			            	CAST(year AS INTEGER) = year_end 
			            	AND id_cc IN (SELECT id FROM TOrgCC) 
			            	AND deleted_at IS NULL
			            GROUP BY
			            	id_gl
			        ) AS CF ON CF.id_gl = GL.id
			        LEFT JOIN (
			            SELECT
			            	id_gl,
			            	SUM(amount) AS SumAmount
			            FROM
			            	Tx_Actual_Opex
			            WHERE
			            	CAST(year AS INTEGER) BETWEEN year_start AND year_end 
			            	AND CAST(month AS INTEGER) BETWEEN month_start AND month_end 
			            	AND id_cc IN (SELECT id FROM TOrgCC) 
			            	AND deleted_at IS NULL
			            GROUP BY
			            	id_gl
			        ) AS ACT ON ACT.id_gl = GL.id
			        LEFT JOIN (
			            SELECT
			            	id_gl,
			            	SUM(amount) AS SumBefore
			            FROM
			            	vw_actual_cost_detail
			            WHERE
			            	CAST(year AS INTEGER) = year_end - 1 
			            	AND id_cc IN (SELECT id FROM TOrgCC)
			            GROUP BY
			            	id_gl
			        ) AS ACTB ON ACTB.id_gl = GL.id
			    ) AS T2
				GROUP BY t_expenses_name, budget_category_id, t_budget_category_name
		) AS t
		WHERE
		    t_actual_before <> 0
		    OR t_budget_amount <> 0
		    OR t_supplement_amount <> 0
		    OR t_carryforward_amount <> 0
		GROUP BY
		    t_level,
		   	t_gl_name,
		   	t_expenses_name,
		   	budget_category_id,
		   	t_budget_category_name,
		   	t_sub_budget_category_name,
		   	t_detail_budget_category_name
	) AS TResult
	ORDER BY
		t_expenses_name ASC NULLS FIRST,
		budget_category_id ASC NULLS FIRST,
		t_sub_budget_category_name ASC NULLS FIRST,
		t_detail_budget_category_name ASC NULLS FIRST
	)
	LOOP
		level := var_r.t_level;
		gl_name := var_r.t_gl_name;
		expenses_name := var_r.t_expenses_name;
		budget_category_name := var_r.t_budget_category_name;
		sub_budget_category_name := var_r.t_sub_budget_category_name;
		detail_budget_category_name := var_r.t_detail_budget_category_name;
		actual_before := var_r.t_actual_before;
		budget_amount := var_r.t_budget_amount;
		supplement_amount := var_r.t_supplement_amount;
		carryforward_amount := var_r.t_carryforward_amount;
		total_budget := var_r.t_total_budget;
		actual_amount := var_r.t_actual_amount;
		actual_percent := var_r.t_actual_percent;
		outstanding_amount := var_r.t_outstanding_amount;
		RETURN NEXT;	
	END LOOP;
END;
$$;