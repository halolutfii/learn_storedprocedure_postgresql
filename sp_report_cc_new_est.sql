-- DROP FUNCTION public.sp_report_cc_new_est(int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION public.sp_report_cc_new_est(start_year integer, month_start integer, end_year integer, month_end integer, _id_cc integer)
 RETURNS TABLE(level text, gl_name text, expenses_name text, budget_category_name text, detail_budget_category_name text, actual_before text, budget_amount text, budget_estimation_amount text, supplement_amount text, carryforward_amount text, total_budget text, actual_amount text, outstanding_amount text)
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
				sub_t_budget_category_name,
				t_detail_budget_category_name,
				SUM(t_actual_before) AS t_actual_before,
				SUM(t_budget_amount) AS t_budget_amount,
				SUM(t_budget_estimation_amount) AS t_budget_estimation_amount,
				SUM(t_supplement_amount) AS t_supplement_amount,
				SUM(t_carryforward_amount) AS t_carryforward_amount,
				SUM(t_budget_amount) + SUM(t_supplement_amount) + SUM(t_carryforward_amount) AS t_total_budget,
				SUM(t_actual_amount) AS t_actual_amount,
				SUM(t_outstanding_amount) AS t_outstanding_amount
			FROM
		    (
				SELECT 
					'1' AS t_level,
					t_expenses_name AS t_gl_name,
					t_expenses_name,
					NULL AS budget_category_id,
					NULL AS t_budget_category_name,
					NULL AS sub_t_budget_category_name,
					NULL AS t_detail_budget_category_name,
					COALESCE(SUM(actualbefore),0) AS t_actual_before,
					COALESCE(SUM(budgetamount),0) AS t_budget_amount,
					COALESCE(SUM(budgetestimationamount),0) AS t_budget_estimation_amount,
					COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
					COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
					COALESCE(SUM(actualamount),0) AS t_actual_amount,
					(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
				FROM (
					Select 
					GL.id,
					GL.gl_code,
					GL.gl_desc,
					GL.nature_coa,
					Mt_Expenses.name AS t_expenses_name,
					Mt_Budget_Category.id AS budget_category_id,
					Mt_Budget_Category.name AS t_budget_category_name,
					Mt_Sub_Budget_Category.name AS sub_t_budget_category_name,
					Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
					CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
					END AS budgetamount,
					CASE
							WHEN BE.SumAmount IS NULL THEN '0'
							ELSE BE.SumAmount
					END AS budgetestimationamount,
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
					from Mt_GL AS GL
					INNER JOIN Mt_Expenses ON GL.expenses = Mt_Expenses.id
					INNER JOIN Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
					INNER JOIN Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS B ON B.id_gl= GL.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget_Estimation
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS BE ON BE.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Suplement_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS SP ON SP.id_gl = GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Carryforward_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS CF ON CF.id_gl = GL.id
					LEFT JOIN (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Tx_Actual_Opex
									where CAST(year AS integer) between start_year and end_year and ( CAST(month AS integer) between month_start and month_end) and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS ACT ON ACT.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumBefore
										FROM vw_actual_cost_detail
										WHERE CAST(year AS integer) = end_year -1  and CAST(id_cc AS integer) = _id_cc
										GROUP BY id_gl
										) AS ACTB ON ACTB.id_gl = GL.id
			) as T1
			GROUP BY t_expenses_name
			UNION ALL
			SELECT 
				'2' AS t_level,
				t_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				NULL AS sub_t_budget_category_name,
				NULL AS t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(budgetestimationamount),0) AS t_budget_estimation_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
			FROM (
					Select 
					GL.id,
					GL.gl_code,
					GL.gl_desc,
					GL.nature_coa,
					Mt_Expenses.name AS t_expenses_name,
					Mt_Budget_Category.id AS budget_category_id,
					Mt_Budget_Category.name AS t_budget_category_name,
					Mt_Sub_Budget_Category.name AS sub_t_budget_category_name,
					Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
					CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
					END AS budgetamount,
					CASE
							WHEN BE.SumAmount IS NULL THEN '0'
							ELSE BE.SumAmount
					END AS budgetestimationamount,
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
					from Mt_GL AS GL
					INNER JOIN Mt_Expenses ON GL.expenses = Mt_Expenses.id
					INNER JOIN Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
					INNER JOIN Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS B ON B.id_gl= GL.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget_Estimation
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS BE ON BE.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Suplement_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS SP ON SP.id_gl = GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Carryforward_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS CF ON CF.id_gl = GL.id
					LEFT JOIN (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Tx_Actual_Opex
									where CAST(year AS integer) between start_year and end_year and ( CAST(month AS integer) between month_start and month_end) and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS ACT ON ACT.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumBefore
										FROM vw_actual_cost_detail
										WHERE CAST(year AS integer) = end_year -1 and CAST(id_cc AS integer) = _id_cc
										GROUP BY id_gl
										) AS ACTB ON ACTB.id_gl = GL.id
			) as T2
			GROUP BY t_expenses_name, budget_category_id, t_budget_category_name
			UNION ALL
			SELECT 
				'3' AS t_level,
				sub_t_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				sub_t_budget_category_name,
				NULL AS t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(budgetestimationamount),0) AS t_budget_estimation_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
			FROM (
					Select 
					GL.id,
					GL.gl_code,
					GL.gl_desc,
					GL.nature_coa,
					Mt_Expenses.name AS t_expenses_name,
					Mt_Budget_Category.id AS budget_category_id,
					Mt_Budget_Category.name AS t_budget_category_name,
					Mt_Sub_Budget_Category.name AS sub_t_budget_category_name,
					Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
					CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
					END AS budgetamount,
					CASE
							WHEN BE.SumAmount IS NULL THEN '0'
							ELSE BE.SumAmount
					END AS budgetestimationamount,
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
					from Mt_GL AS GL
					INNER JOIN Mt_Expenses ON GL.expenses = Mt_Expenses.id
					INNER JOIN Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
					INNER JOIN Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS B ON B.id_gl= GL.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget_Estimation
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS BE ON BE.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Suplement_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS SP ON SP.id_gl = GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Carryforward_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS CF ON CF.id_gl = GL.id
					LEFT JOIN (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Tx_Actual_Opex
									where CAST(year AS integer) between start_year and end_year and ( CAST(month AS integer) between month_start and month_end) and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS ACT ON ACT.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumBefore
										FROM vw_actual_cost_detail
										WHERE CAST(year AS integer) = end_year -1 and CAST(id_cc AS integer) = _id_cc
										GROUP BY id_gl
										) AS ACTB ON ACTB.id_gl = GL.id
			) as T3
			GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, sub_t_budget_category_name
			UNION ALL
			SELECT 
				'4' AS t_level,
				t_detail_budget_category_name AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				sub_t_budget_category_name,
				t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(budgetestimationamount),0) AS t_budget_estimation_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
			FROM (
					Select 
					GL.id,
					GL.gl_code,
					GL.gl_desc,
					GL.nature_coa,
					Mt_Expenses.name AS t_expenses_name,
					Mt_Budget_Category.id AS budget_category_id,
					Mt_Budget_Category.name AS t_budget_category_name,
					Mt_Sub_Budget_Category.name AS sub_t_budget_category_name,
					Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
					CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
					END AS budgetamount,
					CASE
							WHEN BE.SumAmount IS NULL THEN '0'
							ELSE BE.SumAmount
					END AS budgetestimationamount,
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
					from Mt_GL AS GL
					INNER JOIN Mt_Expenses ON GL.expenses = Mt_Expenses.id
					INNER JOIN Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
					INNER JOIN Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS B ON B.id_gl= GL.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget_Estimation
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS BE ON BE.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Suplement_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS SP ON SP.id_gl = GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Carryforward_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS CF ON CF.id_gl = GL.id
					LEFT JOIN (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Tx_Actual_Opex
									where CAST(year AS integer) between start_year and end_year and ( CAST(month AS integer) between month_start and month_end) and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS ACT ON ACT.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumBefore
										FROM vw_actual_cost_detail
										WHERE CAST(year AS integer) = end_year -1 and CAST(id_cc AS integer) = _id_cc
										GROUP BY id_gl
										) AS ACTB ON ACTB.id_gl = GL.id
			) as T4
			GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, sub_t_budget_category_name, t_detail_budget_category_name
			UNION ALL
			SELECT 
				'5' AS t_level,
				gl_desc AS t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				sub_t_budget_category_name,
				t_detail_budget_category_name,
				COALESCE(SUM(actualbefore),0) AS t_actual_before,
				COALESCE(SUM(budgetamount),0) AS t_budget_amount,
				COALESCE(SUM(budgetestimationamount),0) AS t_budget_estimation_amount,
				COALESCE(SUM(supplementamount),0) AS t_supplement_amount,
				COALESCE(SUM(carryforwardamount),0) AS t_carryforward_amount,
				COALESCE(SUM(actualamount),0) AS t_actual_amount,
				(SUM(budgetamount) + SUM(supplementamount) + SUM(carryforwardamount)) - SUM(actualamount) AS t_outstanding_amount
			FROM (
					Select 
					GL.id,
					GL.gl_code,
					GL.gl_desc,
					GL.nature_coa,
					Mt_Expenses.name AS t_expenses_name,
					Mt_Budget_Category.id AS budget_category_id,
					Mt_Budget_Category.name AS t_budget_category_name,
					Mt_Sub_Budget_Category.name AS sub_t_budget_category_name,
					Mt_Detail_Budget_Category.name AS t_detail_budget_category_name,
					CASE
							WHEN B.SumAmount IS NULL THEN '0'
							ELSE B.SumAmount
					END AS budgetamount,
					CASE
							WHEN BE.SumAmount IS NULL THEN '0'
							ELSE BE.SumAmount
					END AS budgetestimationamount,
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
					from Mt_GL AS GL
					INNER JOIN Mt_Expenses ON GL.expenses = Mt_Expenses.id
					INNER JOIN Mt_Budget_Category ON GL.budget_category = Mt_Budget_Category.id
					INNER JOIN Mt_Sub_Budget_Category ON GL.sub_budget_category = Mt_Sub_Budget_Category.id
					INNER JOIN Mt_Detail_Budget_Category ON GL.detail_budget_category = Mt_Detail_Budget_Category.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS B ON B.id_gl= GL.id
					Left Join (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Mt_Budget_Estimation
									where CAST(year AS integer) between start_year and end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS BE ON BE.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Suplement_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS SP ON SP.id_gl = GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumAmount
										FROM Mt_Carryforward_Budget
										WHERE CAST(year AS integer) = end_year and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
										GROUP BY id_gl
										) AS CF ON CF.id_gl = GL.id
					LEFT JOIN (
									select  id_gl
									,       sum(amount) as SumAmount
									from    Tx_Actual_Opex
									where CAST(year AS integer) between start_year and end_year 
									and CAST(month AS integer) between month_start and month_end 
									and CAST(id_cc AS integer) = _id_cc AND deleted_at is NULL
									group by
													id_gl
									) AS ACT ON ACT.id_gl= GL.id
					LEFT JOIN (
									SELECT id_gl
										,			sum(amount) AS SumBefore
										FROM vw_actual_cost_detail
										WHERE CAST(year AS integer) = end_year -1 and CAST(id_cc AS integer) = _id_cc
										GROUP BY id_gl
										) AS ACTB ON ACTB.id_gl = GL.id
			) as T5
			GROUP BY t_expenses_name, budget_category_id, t_budget_category_name, sub_t_budget_category_name, t_detail_budget_category_name, gl_desc) AS t
		WHERE t_actual_before <> 0 OR t_budget_amount <> 0 OR t_budget_estimation_amount <> 0 OR t_supplement_amount <> 0 OR t_carryforward_amount <> 0
		GROUP BY t_level,
				t_gl_name,
				t_expenses_name,
				budget_category_id,
				t_budget_category_name,
				sub_t_budget_category_name,
				t_detail_budget_category_name
		ORDER BY 
				t_expenses_name ASC NULLS FIRST,
				budget_category_id ASC NULLS FIRST,
		-- 		t_budget_category_name ASC NULLS FIRST,
				sub_t_budget_category_name ASC NULLS FIRST,
				t_detail_budget_category_name ASC NULLS FIRST
		
			)
		LOOP
				level := var_r.t_level;
				gl_name := var_r.t_gl_name;
				expenses_name := var_r.t_expenses_name;
				budget_category_name := var_r.t_budget_category_name;
				detail_budget_category_name := var_r.t_detail_budget_category_name;
				actual_before := var_r.t_actual_before;
				budget_amount:= var_r.t_budget_amount;
				budget_estimation_amount := var_r.t_budget_estimation_amount;
				supplement_amount:= var_r.t_supplement_amount;
				carryforward_amount := var_r.t_carryforward_amount;
				total_budget:= var_r.t_total_budget;
				actual_amount := var_r.t_actual_amount;
				outstanding_amount := var_r.t_outstanding_amount;
			
				RETURN NEXT;
				
		END LOOP;

	END$function$
;