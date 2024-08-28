-- DROP FUNCTION public.sp_report_cc(int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION public.sp_report_cc(start_year integer, month_start integer, end_year integer, month_end integer, _id_cc integer)
 RETURNS TABLE(level text, gl_name text, expenses_name text, budget_category_name text, detail_budget_category_name text, actual_before text, budget_amount text, supplement_amount text, carryforward_amount text, total_budget text, actual_amount text, outstanding_amount text)
 LANGUAGE plpgsql
AS $function$
	DECLARE 
    var_r record;
	BEGIN
		-- Routine body goes here...	
		FOR var_r IN(
			SELECT t_level,
				t_gl_name,
				t_expenses_name,
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
			FROM
		    (
		    	-- Level 1
				SELECT '1' AS t_level,
					t_expenses_name AS t_gl_name,
					t_expenses_name,
					NULL AS budget_category_id,
					NULL AS t_budget_category_name,
					NULL AS t_sub_budget_category_name,
					NULL AS t_detail_budget_category_name,
					COALESCE(SUM(actualbefore),0) AS t_actual_before,
					COALESCE(SUM(budgetamount),0) AS t_budget_amount,
					COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
					COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
					COALESCE(SUM(actualamount),0) AS t_actual_amount,
					(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
					FROM (
						-- Main Query for data collection (same as above)
					
					SELECT
						gl.id,
						gl.gl_code,
						gl.gl_desc,
						gl.nature_coa,
						mt_expenses.name AS t_expenses_name,
						mt_budget_category.id AS budget_category_id,
						mt_budget_category.name AS t_budget_category_name,
						mt_sub_budget_category.name AS t_sub_budget_category_name,
						mt_detail_budget_category.name AS t_detail_budget_category_name,
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
						from mt_gl AS gl
						INNER JOIN mt_expenses ON gl.expenses = mt_expenses.id
						INNER JOIN mt_budget_category ON gl.budget_category = mt_budget_category.id
						INNER JOIN mt_sub_budget_category ON gl.sub_budget_category = mt_sub_budget_category.id
						INNER JOIN mt_detail_budget_category ON gl.detail_budget_category = mt_detail_budget_category.id
						Left Join (
										SELECT 
								    id_gl,
								    SUM(amount) as SumAmount
								FROM    
								    mt_budget
								WHERE 
								    CAST(year AS integer) BETWEEN start_year AND end_year
								    AND CAST(id_cc AS integer) = _id_cc
								    AND deleted_at IS NULL
								GROUP BY
								    id_gl
							) AS B ON B.id_gl= gl.id
						LEFT JOIN (
										SELECT 
											id_gl,
											SUM(amount) AS SumAmount
										FROM 
											mt_suplement_budget
										WHERE 
											CAST(year AS integer) = end_year
											AND CAST(id_cc AS integer) = _id_cc
											AND deleted_at IS NULL 
										GROUP BY 
										id_gl
											) AS SP ON SP.id_gl = gl.id
						LEFT JOIN (		
								SELECT
									id_gl,
									SUM(amount) AS SumAmount
								FROM 
									mt_carryforward_budget
								WHERE 
									CAST(year AS integer) = end_year
									AND CAST(id_cc AS integer) = _id_cc
									AND deleted_at is NULL 
									GROUP BY id_gl
											) AS CF ON CF.id_gl = gl.id
						LEFT JOIN (
								SELECT  
								id_gl, 
								SUM(amount) as SumAmount
							FROM    
								tx_actual_opex
							WHERE 
								CAST(year AS integer) BETWEEN start_year and end_year
								AND CAST(month AS integer) BETWEEN month_start and month_end
								AND CAST(id_cc AS integer) = _id_cc
								AND deleted_at is NULL
								GROUP BY id_gl
										) AS ACT ON ACT.id_gl= gl.id
						LEFT JOIN (
								SELECT 
								id_gl,
								SUM(amount) AS SumBefore
							FROM 
								vw_actual_cost_detail
							WHERE 
								CAST(year AS integer) = end_year -1  
								AND CAST(id_cc AS integer) = _id_cc
								GROUP BY id_gl
						) AS ACTB ON ACTB.id_gl = gl.id
						
						) as T1
				GROUP BY t_expenses_name 
				UNION ALL
				
				-- Level 2
				SELECT 
		        '2' AS t_level,
				t_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				NULL AS t_sub_budget_category_name,
				NULL AS t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount

				FROM (
					-- Main Query for data collection (same as above)
					SELECT
					gl.id,
					gl.gl_code,
					gl.gl_desc,
					gl.nature_coa,
					mt_expenses.name AS t_expenses_name,
					mt_budget_category.id AS budget_category_id,
					mt_budget_category.name AS t_budget_category_name,
					mt_sub_budget_category.name AS t_sub_budget_category_name,
					mt_detail_budget_category.name AS t_detail_budget_category_name,
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
					from mt_gl AS gl
					INNER JOIN mt_expenses ON gl.expenses = mt_expenses.id
					INNER JOIN mt_budget_category ON gl.budget_category = mt_budget_category.id
					INNER JOIN mt_sub_budget_category ON gl.sub_budget_category = mt_sub_budget_category.id
					INNER JOIN mt_detail_budget_category ON gl.detail_budget_category = mt_detail_budget_category.id
					Left Join (
									SELECT 
							    id_gl,
							    SUM(amount) as SumAmount
							FROM    
							    mt_budget
							WHERE 
							    CAST(year AS integer) BETWEEN start_year AND end_year
							    AND CAST(id_cc AS integer) = _id_cc
							    AND deleted_at IS NULL
							GROUP BY
							    id_gl
						) AS B ON B.id_gl= gl.id
					LEFT JOIN (
									SELECT 
										id_gl,
										SUM(amount) AS SumAmount
									FROM 
										mt_suplement_budget
									WHERE 
										CAST(year AS integer) = end_year
										AND CAST(id_cc AS integer) = _id_cc
										AND deleted_at IS NULL 
									GROUP BY 
									id_gl
										) AS SP ON SP.id_gl = gl.id
					LEFT JOIN (		
							SELECT
								id_gl,
								SUM(amount) AS SumAmount
							FROM 
								mt_carryforward_budget
							WHERE 
								CAST(year AS integer) = end_year
								AND CAST(id_cc AS integer) = _id_cc
								AND deleted_at is NULL 
								GROUP BY id_gl
										) AS CF ON CF.id_gl = gl.id
					LEFT JOIN (
							SELECT  
							id_gl, 
							SUM(amount) as SumAmount
						FROM    
							tx_actual_opex
						WHERE 
							CAST(year AS integer) BETWEEN start_year and end_year
							AND CAST(month AS integer) BETWEEN month_start and month_end 
							AND CAST(id_cc AS integer) = _id_cc
							AND deleted_at is NULL
							GROUP BY id_gl
									) AS ACT ON ACT.id_gl= gl.id
					LEFT JOIN (
							SELECT 
							id_gl,
							SUM(amount) AS SumBefore
						FROM 
							vw_actual_cost_detail
						WHERE 
							CAST(year AS integer) = end_year -1  
							AND CAST(id_cc AS integer) = _id_cc
							GROUP BY id_gl
					) AS ACTB ON ACTB.id_gl = gl.id
					
					) as T2
					
				GROUP BY t_expenses_name, budget_category_id, t_budget_category_name
				UNION ALL
				
				-- Level 3
				SELECT 
				'3' AS t_level,
				t_sub_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				t_sub_budget_category_name,
				NULL AS t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
				FROM (
				-- Main Query for data collection (same as above)
				SELECT
					gl.id,
					gl.gl_code,
					gl.gl_desc,
					gl.nature_coa,
					mt_expenses.name AS t_expenses_name,
					mt_budget_category.id AS budget_category_id,
					mt_budget_category.name AS t_budget_category_name,
					mt_sub_budget_category.name AS t_sub_budget_category_name,
					mt_detail_budget_category.name AS t_detail_budget_category_name,
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
					from mt_gl AS gl
					INNER JOIN mt_expenses ON gl.expenses = mt_expenses.id
					INNER JOIN mt_budget_category ON gl.budget_category = mt_budget_category.id
					INNER JOIN mt_sub_budget_category ON gl.sub_budget_category = mt_sub_budget_category.id
					INNER JOIN mt_detail_budget_category ON gl.detail_budget_category = mt_detail_budget_category.id
					Left Join (
									SELECT 
							    id_gl,
							    SUM(amount) as SumAmount
							FROM    
							    mt_budget
							WHERE 
							    CAST(year AS integer) BETWEEN start_year AND end_year
							    AND CAST(id_cc AS integer) = _id_cc
							    AND deleted_at IS NULL
							GROUP BY
							    id_gl
						) AS B ON B.id_gl= gl.id
					LEFT JOIN (
									SELECT 
										id_gl,
										SUM(amount) AS SumAmount
									FROM 
										mt_suplement_budget
									WHERE 
										CAST(year AS integer) = end_year
										AND CAST(id_cc AS integer) = _id_cc
										AND deleted_at IS NULL 
									GROUP BY 
									id_gl
										) AS SP ON SP.id_gl = gl.id
					LEFT JOIN (		
							SELECT
								id_gl,
								SUM(amount) AS SumAmount
							FROM 
								mt_carryforward_budget
							WHERE 
								CAST(year AS integer) = end_year
								AND CAST(id_cc AS integer) = _id_cc
								AND deleted_at is NULL 
								GROUP BY id_gl
										) AS CF ON CF.id_gl = gl.id
					LEFT JOIN (
							SELECT  
							id_gl, 
							SUM(amount) as SumAmount
						FROM    
							tx_actual_opex
						WHERE 
							CAST(year AS integer) BETWEEN start_year and end_year
							AND CAST(month AS integer) BETWEEN month_start and month_end 
							AND CAST(id_cc AS integer) = _id_cc
							AND deleted_at is NULL
							GROUP BY id_gl
									) AS ACT ON ACT.id_gl= gl.id
					LEFT JOIN (
							SELECT 
							id_gl,
							SUM(amount) AS SumBefore
						FROM 
							vw_actual_cost_detail
						WHERE 
							CAST(year AS integer) = end_year -1  
							AND CAST(id_cc AS integer) = _id_cc
							GROUP BY id_gl
					) AS ACTB ON ACTB.id_gl = gl.id
					
					) as T3
					
					GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, t_sub_budget_category_name
			UNION ALL
			
			-- Level 4
			SELECT 
				'4' AS t_level,
				t_detail_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				t_sub_budget_category_name,
				t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
				FROM (
				-- Main Query for data collection (same as above)
				SELECT
					gl.id,
					gl.gl_code,
					gl.gl_desc,
					gl.nature_coa,
					mt_expenses.name AS t_expenses_name,
					mt_budget_category.id AS budget_category_id,
					mt_budget_category.name AS t_budget_category_name,
					mt_sub_budget_category.name AS t_sub_budget_category_name,
					mt_detail_budget_category.name AS t_detail_budget_category_name,
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
					from mt_gl AS gl
					INNER JOIN mt_expenses ON gl.expenses = mt_expenses.id
					INNER JOIN mt_budget_category ON gl.budget_category = mt_budget_category.id
					INNER JOIN mt_sub_budget_category ON gl.sub_budget_category = mt_sub_budget_category.id
					INNER JOIN mt_detail_budget_category ON gl.detail_budget_category = mt_detail_budget_category.id
					Left Join (
									SELECT 
							    id_gl,
							    SUM(amount) as SumAmount
							FROM    
							    mt_budget
							WHERE 
							    CAST(year AS integer) BETWEEN start_year AND end_year
							    AND CAST(id_cc AS integer) = _id_cc
							    AND deleted_at IS NULL
							GROUP BY
							    id_gl
						) AS B ON B.id_gl= gl.id
					LEFT JOIN (
									SELECT 
										id_gl,
										SUM(amount) AS SumAmount
									FROM 
										mt_suplement_budget
									WHERE 
										CAST(year AS integer) = end_year
										AND CAST(id_cc AS integer) = _id_cc
										AND deleted_at IS NULL 
									GROUP BY 
									id_gl
										) AS SP ON SP.id_gl = gl.id
					LEFT JOIN (		
							SELECT
								id_gl,
								SUM(amount) AS SumAmount
							FROM 
								mt_carryforward_budget
							WHERE 
								CAST(year AS integer) = end_year
								AND CAST(id_cc AS integer) = _id_cc
								AND deleted_at is NULL 
								GROUP BY id_gl
										) AS CF ON CF.id_gl = gl.id
					LEFT JOIN (
							SELECT  
							id_gl, 
							SUM(amount) as SumAmount
						FROM    
							tx_actual_opex
						WHERE 
							CAST(year AS integer) BETWEEN start_year and end_year
							AND CAST(month AS integer) BETWEEN month_start and month_end 
							AND CAST(id_cc AS integer) = _id_cc
							AND deleted_at is NULL
							GROUP BY id_gl
									) AS ACT ON ACT.id_gl= gl.id
					LEFT JOIN (
							SELECT 
							id_gl,
							SUM(amount) AS SumBefore
						FROM 
							vw_actual_cost_detail
						WHERE 
							CAST(year AS integer) = end_year -1  
							AND CAST(id_cc AS integer) = _id_cc
							GROUP BY id_gl
					) AS ACTB ON ACTB.id_gl = gl.id
					
					) as T4
					
					GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, t_sub_budget_category_name, t_detail_budget_category_name
			UNION ALL
			
			-- Level 5
			SELECT 
				'5' AS t_level,
				gl_desc AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				t_sub_budget_category_name,
				t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
					FROM (
					-- Main Query for data collection (same as above)
				SELECT
					gl.id,
					gl.gl_code,
					gl.gl_desc,
					gl.nature_coa,
					mt_expenses.name AS t_expenses_name,
					mt_budget_category.id AS budget_category_id,
					mt_budget_category.name AS t_budget_category_name,
					mt_sub_budget_category.name AS t_sub_budget_category_name,
					mt_detail_budget_category.name AS t_detail_budget_category_name,
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
					from mt_gl AS gl
					INNER JOIN mt_expenses ON gl.expenses = mt_expenses.id
					INNER JOIN mt_budget_category ON gl.budget_category = mt_budget_category.id
					INNER JOIN mt_sub_budget_category ON gl.sub_budget_category = mt_sub_budget_category.id
					INNER JOIN mt_detail_budget_category ON gl.detail_budget_category = mt_detail_budget_category.id
					Left Join (
									SELECT 
							    id_gl,
							    SUM(amount) as SumAmount
							FROM    
							    mt_budget
							WHERE 
							    CAST(year AS integer) BETWEEN start_year AND end_year
							    AND CAST(id_cc AS integer) = _id_cc
							    AND deleted_at IS NULL
							GROUP BY
							    id_gl
						) AS B ON B.id_gl= gl.id
					LEFT JOIN (
									SELECT 
										id_gl,
										SUM(amount) AS SumAmount
									FROM 
										mt_suplement_budget
									WHERE 
										CAST(year AS integer) = end_year
										AND CAST(id_cc AS integer) = _id_cc
										AND deleted_at IS NULL 
									GROUP BY 
									id_gl
										) AS SP ON SP.id_gl = gl.id
					LEFT JOIN (		
							SELECT
								id_gl,
								SUM(amount) AS SumAmount
							FROM 
								mt_carryforward_budget
							WHERE 
								CAST(year AS integer) = end_year
								AND CAST(id_cc AS integer) = _id_cc
								AND deleted_at is NULL 
								GROUP BY id_gl
										) AS CF ON CF.id_gl = gl.id
					LEFT JOIN (
							SELECT  
							id_gl, 
							SUM(amount) as SumAmount
						FROM    
							tx_actual_opex
						WHERE 
							CAST(year AS integer) BETWEEN start_year and end_year
							AND CAST(month AS integer) BETWEEN month_start and month_end 
							AND CAST(id_cc AS integer) = _id_cc
							AND deleted_at is NULL
							GROUP BY id_gl
									) AS ACT ON ACT.id_gl= gl.id
					LEFT JOIN (
							SELECT 
							id_gl,
							SUM(amount) AS SumBefore
						FROM 
							vw_actual_cost_detail
						WHERE 
							CAST(year AS integer) = end_year -1  
							AND CAST(id_cc AS integer) = _id_cc
							GROUP BY id_gl
					) AS ACTB ON ACTB.id_gl = gl.id
					
					) as T5
					
					GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, t_sub_budget_category_name, t_detail_budget_category_name, gl_desc) AS t
		WHERE t_actual_before <> 0 OR t_budget_amount <> 0 OR t_supplement_amount <> 0 OR t_carryforward_amount <> 0
		GROUP BY t_level,
				t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				t_sub_budget_category_name,
				t_detail_budget_category_name
		ORDER BY 
				t_expenses_name ASC NULLS FIRST,
				budget_category_id ASC NULLS FIRST,
		-- 		t_budget_category_name ASC,
				t_sub_budget_category_name ASC NULLS FIRST,
				t_detail_budget_category_name ASC NULLS first)
		LOOP
				level := var_r.t_level;
				gl_name := var_r.t_gl_name;
				expenses_name := var_r.t_expenses_name;
				budget_category_name := var_r.t_budget_category_name;
				detail_budget_category_name := var_r.t_detail_budget_category_name;
				actual_before := var_r.t_actual_before;
				budget_amount := var_r.t_budget_amount;
				supplement_amount := var_r.t_supplement_amount;
				carryforward_amount := var_r.t_carryforward_amount;
				total_budget := var_r.t_total_budget;
				actual_amount := var_r.t_actual_amount;
				outstanding_amount := var_r.t_outstanding_amount;
			
				RETURN NEXT;
				
		END LOOP;

	END$function$
;
