WITH
-- ФИЛЬТР ЦЕНЫ
filter_price AS (
  SELECT distinct ArticleNumber, 
         MerchantID
  FROM project.input.FactOfferStatistic
  WHERE Price <= 450
),
-- ФИЛЬТР КАТЕГОРИИ
filter_category AS (
  SELECT item_id, 
          item_article_name,
          item_business_group_name,
          item_master_level1_name,
          item_master_level2_name,
          item_master_level3_name
  FROM project.adhoc.gp_rep_item_gp
  WHERE item_web_level1_name IN ('Книги, хобби, канцелярия', 'Детские товары','Товары для дома', 'Одежда, обувь и аксессуары', 'Красота и уход')
),
-- ФИЛЬТР СТОКА (остатков) и ИНФОРМАЦИЯ ОБ ОСТАТКАХ
filter_stock AS (
  SELECT item_id,
         merchant_id, 
         quantity_amount,
         item_name
  FROM project.input.hdfs_stock_hist_backup
  WHERE quantity_amount > 0
  AND update_date = '2024-08-24'
  AND quality = 'GENERAL'
  AND item_id IS NOT null
  ORDER BY item_id ASC ),

-- ИМЕНА МЕРЧАНТОВ
names_merch AS (
  SELECT distinct MerchantID, 
         MerchantFullName
  FROM project.input.DimMerchant
),

-- GMV за последние 2 недели
gmv_2p_2_nedeli AS (
  select distinct fop.ArticleNumber, 
         SUM(fop.GMV) AS gmv_2p
    FROM project.input.oms_FactOrderPosition fop
    LEFT JOIN project.input.DimShipmentOrderDetail dsod ON fop.orderid = cast(dsod.orderid as string)
    
    WHERE fop.lot_type_key = 2
      AND fop.isexcluded = False
      AND CAST(fop.LotCloseDate AS DATE) BETWEEN DATE '2024-08-12' AND DATE '2024-08-26'
      AND dsod.shipment_business_type = '2p'
    GROUP BY fop.ArticleNumber
),

-- Конверсия из просмотра карточки в покупку

-- TR категории (если получится)
tb_tr_category AS (
SELECT 
bg,
sum(tf) as tf
FROM (select distinct fop.LotID, 
      transactionfee tf,
      it.item_business_group_name bg
    FROM project.input.oms_FactOrderPosition fop
    LEFT JOIN project.input.DimShipmentOrderDetail dsod ON fop.orderid = cast(dsod.orderid as string)
    LEFT JOIN project.adhoc.gp_rep_item_gp it ON fop.ArticleNumber = it.item_id
    WHERE fop.lot_type_key = 2
      AND fop.isexcluded = False
      AND CAST(fop.LotCloseDate AS DATE) BETWEEN DATE '2024-08-12' AND DATE '2024-08-26'
      AND dsod.shipment_business_type = '2p'
) t1
GROUP BY 1
),

--ФИЛЬТР ПО СПИСКУ ЗАКАЗЧИКА
tb_zakazchik AS (
  SELECT item_id, merchant_id
  FROM project.user.number
),

-- ОСНОВНОЙ ЗАПРОС
main_tb AS (SELECT
item_business_group_name as BG, 
item_master_level1_name as ML1, 
item_master_level2_name as ML2, 
item_master_level3_name as ML3,
fp.ArticleNumber as id_product,
item_article_name AS name_product,
nm.MerchantID AS merchant_id,
MerchantFullName, 
COALESCE(gmv_2p_2_nedeli.gmv_2p,0) AS GMV_product,
--Conversion,
quantity_amount as stock,
tb_tr.tf as tr_category

FROM filter_price fp
JOIN filter_category fc ON fp.ArticleNumber = fc.item_id  
JOIN filter_stock fs ON fp.ArticleNumber = fs.item_id AND fp.MerchantID = fs.merchant_id
JOIN tb_zakazchik tz ON fp.ArticleNumber = tz.item_id AND fp.MerchantID = tz.merchant_id

LEFT JOIN names_merch nm ON fp.MerchantID = nm.MerchantID
LEFT JOIN gmv_2p_2_nedeli ON fp.ArticleNumber = gmv_2p_2_nedeli.ArticleNumber
LEFT JOIN tb_tr_category tb_tr ON fc.item_business_group_name = tb_tr.bg
)

SELECT 
    BG,
    ML1,
    ML2,
    ML3,
    id_product,
    name_product,
    merchant_id
    GMV_product,
    stock,
    tr_category,
    ROW_NUMBER() OVER (
        ORDER BY 
            GMV_product DESC, 
            stock DESC, 
            tr_category DESC
    ) AS overall_rank
FROM 
    main_tb
ORDER BY 
    overall_rank;
