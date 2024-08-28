CREATE OR REPLACE FUNCTION "public"."get_total_opex_department"(
	div_id int,
	period int,
	month_period int
)
RETURNS TABLE(
	"id" int,
	"dept_name" text,
	"actual" decimal(18,2),
	"budget" decimal(18,2),
	"actual_percent" decimal(18,2)
) AS $BODY$
DECLARE 
    var_r record;
    org_name_ VARCHAR(50);
BEGIN
		
	DROP TABLE IF EXISTS org_;
	DROP TABLE IF EXISTS tbl_hc_gdh;
	DROP TABLE IF EXISTS tbl_opex;

	CREATE TEMP TABLE org_ (org_id INT);
	
	CREATE TEMP TABLE tbl_hc_gdh (
		id INT,
		dept_name VARCHAR(200),
		actual DECIMAL(19,1),
		budget DECIMAL(19,1),
		actual_percent DECIMAL(19,1)
	);
	
	CREATE TEMP TABLE tbl_opex (
		id INT,
		dept_name VARCHAR(200),
		actual DECIMAL(19,1),
		budget DECIMAL(19,1),
		actual_percent DECIMAL(19,1)
	);

    SELECT
        Mt_Organization_cc.org_name
    INTO
        org_name_
    FROM
        Mt_Organization_cc
    WHERE
        Mt_Organization_cc.id = div_id;
	
	INSERT INTO org_
    SELECT
		id_org
	FROM
		Mt_OrgHierarchy_cc
	WHERE
	id_org_parent = div_id;

			IF org_name_ = 'HC' THEN
     			WITH TBL_OPEX_HC AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_opex) AS t_total_opex
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_opex
	       					FROM
	       						Tx_Actual_Opex a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = 4
	       					AND
	       						CAST(a."year" AS INTEGER) = PERIOD
	       					AND
	       						CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						d.org_shortname,
	       						a."year"
	       				) x
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       					)
	       				AS x
	       			),
	       			
       			TBL_BUDGET_HC AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_budget) AS t_total_budget
	       					
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_budget
	       					FROM
	       						Mt_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = 4
	       					AND
	       						CAST(a."year" AS INTEGER) = period
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						d.org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_SUPLEMENT_HC AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_suplement) AS t_total_suplement
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_suplement
	       					FROM
	       						Mt_Suplement_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = 4
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						((CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period) OR CAST(a."month" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_CARRYFORWARD_HC AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_carryforward) AS t_total_carryforward
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_carryforward
	       					FROM
	       						Mt_Carryforward_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = 4
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
	       		),
	       			
       			TBL_CALC_BUDGET_HC AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					a.t_id AS t_id,
	       					a.t_dept_name AS t_dept_name,
	       					(
	       						COALESCE(a.t_total_budget, 0) +
	       						COALESCE(b.t_total_suplement, 0) +
	       						COALESCE(c.t_total_carryforward, 0)
	   						) AS t_budget
	       				FROM
							TBL_BUDGET_HC a
						INNER JOIN
							TBL_SUPLEMENT_HC b
						ON
							a.t_id = b.t_id
						INNER JOIN
							TBL_CARRYFORWARD_HC c
						ON
							a.t_id = c.t_id
						
					) AS x
       			)
       			
       			INSERT INTO tbl_hc_gdh
       			SELECT
       				b.t_id AS t_id,
       				b.t_dept_name AS t_dept_name,
       				ROUND(COALESCE(a.t_total_opex,0) / 1000000, 1) AS t_actual,
					ROUND(COALESCE(b.t_budget,0) / 1000000, 1) AS t_budget,
       				CASE 
    					WHEN b.t_budget = 0 THEN 0
    					ELSE ((COALESCE(a.t_total_opex,0) / b.t_budget) * 100)
					END AS t_actual_percent
				FROM
					TBL_OPEX_HC a
				RIGHT JOIN
					TBL_CALC_BUDGET_HC b
				ON
					a.t_id = b.t_id;
				
       			WITH TBL_OPEX AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_opex) AS t_total_opex
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_opex
	       					FROM
	       						Tx_Actual_Opex a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org_parent
	       					WHERE
	       						c.id_org_parent
	       					IN
	       						(SELECT org_id FROM org_)
	       					AND
	       						CAST(a."year" AS INTEGER) = PERIOD
	       					AND
	       						CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_budget) AS t_total_budget
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_budget
	       					FROM
	       						Mt_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org_parent
	       					WHERE
	       						c.id_org_parent
	       					IN
	       						(SELECT org_id FROM org_)
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
       				) AS x
       			),
       			
       			TBL_SUPLEMENT AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_suplement) AS t_total_suplement
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_suplement
	       					FROM
	       						Mt_Suplement_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org_parent
	       					WHERE
	       						c.id_org_parent
	       					IN
	       						(SELECT org_id FROM org_)
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						((CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period) OR CAST(a."month" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_CARRYFORWARD AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_carryforward) AS t_total_carryforward
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_carryforward
	       					FROM
	       						Mt_Carryforward_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org_parent
	       					WHERE
	       						c.id_org_parent
	       					IN
	       						(SELECT org_id FROM org_)
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       				) AS x
       			),
       			
       			TBL_CALC_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					a.t_id,
	       					a.t_dept_name,
	       					(
	       						COALESCE(a.t_total_budget, 0) +
	       						COALESCE(b.t_total_suplement, 0) +
	       						COALESCE(c.t_total_carryforward, 0)
									) AS t_budget
									FROM
									TBL_BUDGET a
									INNER JOIN
									TBL_SUPLEMENT b
									ON
										a.t_id = b.t_id
									INNER JOIN
										TBL_CARRYFORWARD c
									ON
										a.t_id = c.t_id
								) AS x
       			)
       			
       			INSERT INTO tbl_opex
       			SELECT
       				b.t_id,
       				b.t_dept_name,
       				ROUND(COALESCE(a.t_total_opex, 0) / 1000000, 1) AS t_actual,
       				ROUND(COALESCE(b.t_budget, 0) / 1000000, 1) AS t_budget,
       				CASE 
    					WHEN b.t_budget = 0 THEN 0
    					ELSE ((COALESCE(a.t_total_opex,0) / b.t_budget) * 100)
					END AS t_actual_percent
       			FROM
       				TBL_OPEX a
       			RIGHT JOIN
       				TBL_CALC_BUDGET b
       			ON
       				a.t_id = b.t_id
       			ORDER BY
       				b.t_budget ASC;
       	
			 ELSIF org_name_ != 'HC GDH' THEN
       			WITH TBL_OPEX AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_opex) AS t_total_opex
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_opex
	       					FROM
	       						Tx_Actual_Opex a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org
	       					WHERE
	       						c.id_org_parent = div_id
	       					AND
	       						CAST(a."year" AS INTEGER) = PERIOD
	       					AND
	       						CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_budget) AS t_total_budget
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_budget
	       					FROM
	       						Mt_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org
	       					WHERE
	       						c.id_org_parent = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
       				) AS x
       			),
       			
       			TBL_SUPLEMENT AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_suplement) AS t_total_suplement
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_suplement
	       					FROM
	       						Mt_Suplement_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org
	       					WHERE
	       						c.id_org_parent = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						((CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period) OR CAST(a."month" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_CARRYFORWARD AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_carryforward) AS t_total_carryforward
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_carryforward
	       					FROM
	       						Mt_Carryforward_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_OrgHierarchy_CC c
	       					ON
	       						c.id_org = b.id_div_dep
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = c.id_org
	       					WHERE
	       						c.id_org_parent = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       				) AS x
       			),
       			
       			TBL_CALC_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					a.t_id,
	       					a.t_dept_name,
	       					(
	       						COALESCE(a.t_total_budget, 0) +
	       						COALESCE(b.t_total_suplement, 0) +
	       						COALESCE(c.t_total_carryforward, 0)
									) AS t_budget
									FROM
									TBL_BUDGET a
									INNER JOIN
									TBL_SUPLEMENT b
									ON
										a.t_id = b.t_id
									INNER JOIN
										TBL_CARRYFORWARD c
									ON
										a.t_id = c.t_id
								) AS x
       			)
       			
       			INSERT INTO tbl_opex
       			SELECT
       				b.t_id,
       				b.t_dept_name,
       				COALESCE(a.t_total_opex, 0) / 1000000 AS t_actual,
       				COALESCE(b.t_budget, 0) / 1000000 AS t_budget,
       				CASE 
    					WHEN b.t_budget = 0 THEN 0
    					ELSE ((COALESCE(a.t_total_opex,0) / b.t_budget) * 100)
					END AS t_actual_percent
       			FROM
       				TBL_OPEX a
       			RIGHT JOIN
       				TBL_CALC_BUDGET b
       			ON
       				a.t_id = b.t_id
       			ORDER BY
       				b.t_budget ASC;
       			
			 ELSE
       			WITH TBL_OPEX AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_opex) AS t_total_opex
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_opex
	       					FROM
	       						Tx_Actual_Opex a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = div_id
	       					AND
	       						CAST(a."year" AS INTEGER) = PERIOD
	       					AND
	       						CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_budget) AS t_total_budget
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_budget
	       					FROM
	       						Mt_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
       				) AS x
       			),
       			
       			TBL_SUPLEMENT AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_suplement) AS t_total_suplement
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_suplement
	       					FROM
	       						Mt_Suplement_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						((CAST(a."month" AS INTEGER) BETWEEN 1 AND month_period) OR CAST(a."month" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       			) AS x
       			),
       			
       			TBL_CARRYFORWARD AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					t_id,
	       					t_dept_name,
	       					MAX(t_total_carryforward) AS t_total_carryforward
	       				FROM
	       				(
	       					SELECT
	       						d.id AS t_id,
	       						d.org_shortname AS t_dept_name,
	       						a."year" AS t_year,
	       						SUM(a.amount) AS t_total_carryforward
	       					FROM
	       						Mt_Carryforward_Budget a
	       					RIGHT OUTER JOIN
	       						Mt_CC b
	       					ON
	       						a.id_cc = b.id
	       					RIGHT OUTER JOIN
	       						Mt_Organization_CC d
	       					ON
	       						d.id = b.id_div_dep
	       					WHERE
	       						d.id = div_id
	       					AND
	       						(CAST(a."year" AS INTEGER) = period OR CAST(a."year" AS INTEGER) IS NULL)
	       					AND
	       						a.deleted_at IS NULL
	       					GROUP BY
	       						d.id,
	       						org_shortname,
	       						a."year"
	       				) x
	       				
	       				GROUP BY
	       					t_id,
	       					t_dept_name
	       				) AS x
       			),
       			
       			TBL_CALC_BUDGET AS (
       				SELECT * FROM
       				(
	       				SELECT
	       					a.t_id,
	       					a.t_dept_name,
	       					(
	       						COALESCE(a.t_total_budget, 0) +
	       						COALESCE(b.t_total_suplement, 0) +
	       						COALESCE(c.t_total_carryforward, 0)
									) AS t_budget
									FROM
									TBL_BUDGET a
									INNER JOIN
									TBL_SUPLEMENT b
									ON
										a.t_id = b.t_id
									INNER JOIN
										TBL_CARRYFORWARD c
									ON
										a.t_id = c.t_id
								) AS x
       			)
       			
       			INSERT INTO tbl_opex
       			SELECT
       				b.t_id,
       				b.t_dept_name,
       				ROUND(COALESCE(a.t_total_opex, 0) / 1000000, 1) AS t_actual,
       				ROUND(COALESCE(b.t_budget, 0) / 1000000, 1) AS t_budget,
       				CASE 
    					WHEN b.t_budget = 0 THEN 0
    					ELSE ((COALESCE(a.t_total_opex,0) / b.t_budget) * 100)
					END AS t_actual_percent
       			FROM
       				TBL_OPEX a
       			RIGHT JOIN
       				TBL_CALC_BUDGET b
       			ON
       				a.t_id = b.t_id
       			ORDER BY
       				b.t_budget ASC;
       		
       		END IF;
       				
	FOR var_r IN (
       			SELECT *
       			FROM
       			(
       				SELECT
       					tbl_hc_gdh.id,
       					tbl_hc_gdh.dept_name,
       					tbl_hc_gdh.actual,
       					tbl_hc_gdh.budget,
       					tbl_hc_gdh.actual_percent
       				FROM
       					tbl_hc_gdh
       				WHERE
            			tbl_hc_gdh.actual <> 0 OR tbl_hc_gdh.budget <> 0
       				UNION
       				SELECT
       					tbl_opex.id,
       					tbl_opex.dept_name,
       					tbl_opex.actual,
       					tbl_opex.budget,
       					tbl_opex.actual_percent
       				FROM
       					tbl_opex
        			WHERE
            			tbl_opex.actual <> 0 OR tbl_opex.budget <> 0
       			) AS "result"
       			ORDER BY budget
	)
	LOOP
		id := var_r.id;
		dept_name := var_r.dept_name;
		actual := var_r.actual;
		budget := var_r.budget;
		actual_percent := var_r.actual_percent;
		RETURN NEXT;	
	END LOOP;	
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100