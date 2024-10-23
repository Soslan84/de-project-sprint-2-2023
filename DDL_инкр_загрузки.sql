 --DDL инкрементальных загрузок

with
/* выбираем дельту изменений */
dwh_delta AS (
    SELECT     
            dcs.customer_id AS customer_id,				--данные покупателей(основа витрины в нашем случае)
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            fo.order_id AS order_id,				--будет необходим чтоыб считать кол-во заказов у покупателя
            dp.product_id AS product_id,			--для определения кол-ва потраченых денег, лучше конечно иметь в таблице фактов конкретную сумму по каждому заказазу
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            fo.order_completion_date - fo.order_created_date AS diff_order_date, --статусы заказов , нужны для витрины
            fo.order_status AS order_status,
            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,							--загрузки
            dc.load_dttm AS craftsman_load_dttm,							
            dcs.load_dttm AS customers_load_dttm,
            dp.load_dttm AS products_load_dttm,
            dc.craftsman_id  as craftsman_id
            FROM dwh.f_order fo 
                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
                LEFT JOIN dwh.customer_report_datamart crd ON dc.craftsman_id = crd.customer_id
                    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
dwh_update_delta AS (  --выбираем дельту для обновления, сейчас она должна быть пустой, так как витрина не заполнена
    SELECT     
            dd.exist_customer_id AS customer_id
            FROM dwh_delta dd 
             WHERE dd.exist_customer_id IS NOT null
             ),
top_product as (---для определения топ категории продукта
select 
	customer_id,
	product_type as top_product_category
	from		--несколько вложенных запросов дял определения топ продукта у клиента
		(
		select customer_id,			--в этом подзапрсое расставляем ранги
		product_type,
		RANK() OVER(PARTITION BY customer_id ORDER BY count_product DESC) AS rank_count_product
		from (
			SELECT 						--в этом подзапрсое считаем категории
			customer_id AS customer_id, 
			product_type, 
			COUNT(product_id) AS count_product
			FROM dwh_delta 
			GROUP BY customer_id, product_type
			ORDER BY count_product desc
			) as count_product
		) as rank_product
		where rank_count_product=1
),
top_craftsman as ( --определяем топ мастера у клиента, аналогично предыдущему CTE, только заменим rank на ROW_NUMBER
select
	customer_id,
	craftsman_id as top_craftsman_id
	from (
			select 
			ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY count_craftsman DESC) AS rank_count_craftsman,
			customer_id,
			craftsman_id
			from (
				SELECT 						
				customer_id AS customer_id, 
				craftsman_id, 
				COUNT(craftsman_id) AS count_craftsman
				FROM dwh_delta 
				GROUP BY customer_id,craftsman_id
				ORDER BY count_craftsman desc
			) as count_craftsman 
			) as rank_craftsman
			where rank_count_craftsman=1
),
---далее подготовим новые данные
dwh_delta_insert_result_T1 as (			--основные данные
	SELECT 
		customer_id AS customer_id,
		customer_name AS customer_name,
		customer_address AS customer_address,
		customer_birthday AS customer_birthday,
		customer_email AS customer_email,
		SUM(product_price) AS customer_money,
		SUM(product_price) * 0.1 AS platform_money,
		COUNT(order_id) AS count_order,
		AVG(product_price) AS avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
		SUM(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		SUM(CASE WHEN order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		report_period AS report_period
FROM dwh_delta 
WHERE exist_customer_id IS NULL
GROUP BY customer_id, customer_name, customer_address, customer_birthday, customer_email, report_period
),
dwh_delta_insert_result as ( --обьеденяем CTE для итоговой таблицы dwh_delta_insert_result
	select 
		T1.customer_id AS customer_id,
		T1.customer_name AS customer_name,
		T1.customer_address AS customer_address,
		T1.customer_birthday AS customer_birthday,
		T1.customer_email AS customer_email,
		T1.customer_money as customer_money,
		T1.platform_money as platform_money,
		T1.count_order as count_order,
		T1.avg_price_order as avg_price_order,
		T1.median_time_order_completed as median_time_order_completed,
		T1.count_order_created as count_order_created,
		T1.count_order_in_progress as count_order_in_progress, 
		T1.count_order_delivery as count_order_delivery, 
		T1.count_order_done as count_order_done, 
		T1.count_order_not_done as count_order_not_done,
		T1.report_period AS report_period,
		T2.top_product_category as top_product_category,
		T3.top_craftsman_id as top_craftsman_id
from dwh_delta_insert_result_T1 as T1
inner join top_product as T2 on T1.customer_id = T2.customer_id
inner join top_craftsman as T3 on T1.customer_id = T3.customer_id
),
dwh_delta_update_result_T1 as  (--данные для апдейта, для топовых категорий продуктов и топовых мастеров CTE уже есть
	select 
		customer_id AS customer_id,
		customer_name AS customer_name,
		customer_address AS customer_address,
		customer_birthday AS customer_birthday,
		customer_email AS customer_email,
		SUM(product_price) AS customer_money,
		SUM(product_price) * 0.1 AS platform_money,
		COUNT(order_id) AS count_order,
		AVG(product_price) AS avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
		SUM(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		SUM(CASE WHEN order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		report_period AS report_period
from (
SELECT     -- в этой выборке достаём из DWH данные , которые уже есть в витрине
	dcs.customer_id AS customer_id,
	dcs.customer_name AS customer_name,
	dcs.customer_address AS customer_address,
	dcs.customer_birthday AS customer_birthday,
	dcs.customer_email AS customer_email,
	fo.order_id AS order_id,
    dp.product_id AS product_id,
    dp.product_price AS product_price,
    dp.product_type AS product_type,
    fo.order_completion_date - fo.order_created_date AS diff_order_date,
    fo.order_status AS order_status, 
    TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
    FROM dwh.f_order fo 
    INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
    INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id    --то что уже есть дял обновления
   ) as T1
   GROUP BY customer_id, customer_name, customer_address, customer_birthday, customer_email, report_period
   ),
dwh_delta_update_result as ( --обьеденяем CTE для итоговой таблицы dwh_delta_insert_result
	select 
		T1.customer_id AS customer_id,
		T1.customer_name AS customer_name,
		T1.customer_address AS customer_address,
		T1.customer_birthday AS customer_birthday,
		T1.customer_email AS customer_email,
		T1.customer_money as customer_money,
		T1.platform_money as platform_money,
		T1.count_order as count_order,
		T1.avg_price_order as avg_price_order,
		T1.median_time_order_completed as median_time_order_completed,
		T1.count_order_created as count_order_created,
		T1.count_order_in_progress as count_order_in_progress, 
		T1.count_order_delivery as count_order_delivery, 
		T1.count_order_done as count_order_done, 
		T1.count_order_not_done as count_order_not_done,
		T1.report_period AS report_period,
		T2.top_product_category as top_product_category,
		T3.top_craftsman_id as top_craftsman_id
from dwh_delta_update_result_T1 as T1
inner join top_product as T2 on T1.customer_id = T2.customer_id
inner join top_craftsman as T3 on T1.customer_id = T3.customer_id
),
insert_delta AS ( -- выполняем insert новых расчитанных данных для витрины 
    INSERT INTO dwh.customer_report_datamart  (			
        customer_id ,
        customer_name ,
        customer_address,
        customer_birthday, 
        customer_email, 
        customer_money, 
        platform_money, 
        count_order, 
        avg_price_order, 
        median_time_order_completed,
        top_product_category, 
        top_craftsman_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
    ) SELECT 
        customer_id ,
        customer_name ,
        customer_address,
        customer_birthday, 
        customer_email, 
        customer_money, 
        platform_money, 
        count_order, 
        avg_price_order, 
        median_time_order_completed,
        top_product_category, 
        top_craftsman_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
            FROM dwh_delta_insert_result
),
update_delta AS ( -- выполняем обновление показателей в отчёте по уже существующим мастерам
    UPDATE dwh.customer_report_datamart SET
        customer_name = upd.customer_name ,
        customer_address = upd.customer_address,
        customer_birthday = upd.customer_birthday, 
        customer_email = upd.customer_email, 
        customer_money = upd.customer_money, 
        platform_money = upd.platform_money, 
        count_order = upd.count_order, 
        avg_price_order = upd.avg_price_order , 
        median_time_order_completed = upd.median_time_order_completed,
        top_product_category = upd.top_product_category, 
        top_craftsman_id = upd.top_craftsman_id,
        count_order_created = upd.count_order_created, 
        count_order_in_progress = upd.count_order_in_progress, 
        count_order_delivery = upd.count_order_delivery, 
        count_order_done = upd.count_order_done, 
        count_order_not_done = upd.count_order_not_done, 
        report_period = upd.report_period
    FROM (
        SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday, 
        customer_email, 
        customer_money, 
        platform_money, 
        count_order, 
        avg_price_order, 
        median_time_order_completed,
        top_product_category, 
        top_craftsman_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period 
        FROM dwh_delta_update_result) AS upd
    WHERE dwh.customer_report_datamart.customer_id = upd.customer_id
)
select 'INIT' --для запуска
;




             

