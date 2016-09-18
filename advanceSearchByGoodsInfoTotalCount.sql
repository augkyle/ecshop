DROP PROCEDURE IF EXISTS `advanceSearchByGoodsInfoTotalCount`$$

CREATE PROCEDURE `advanceSearchByGoodsInfoTotalCount`(
	goodsInfo varchar(200),
    filteredCatId bigint,
    filteredGoodsBrandId int, 
    filteredVehicleId int,
	currentVehicleId int,
    filteredAttributes varchar(200))
BEGIN

CREATE TEMPORARY TABLE categoryList
(
    cat_id int
);

CREATE TEMPORARY TABLE tempGoodsInfos
(
	goods_id bigint,
	brief_s_code varchar(120), 
	brand_name nvarchar(100),
    is_common tinyint
);

CREATE TEMPORARY TABLE goodsInfos
(
	goods_id bigint
);

CREATE TEMPORARY TABLE attributeInfos
(
    attr_id int,
    attr_name nvarchar(50),
    attr_value nvarchar(100)
);

INSERT INTO categoryList
(
    cat_id
)
SELECT
    cat.cat_id
FROM ecs_category as cat
WHERE cat.cat_name LIKE CONCAT('%', goodsInfo, '%')
UNION
SELECT
    alias.cat_id
FROM skb_vehicle_accessory_alias AS alias
WHERE alias.accessory_alias_value LIKE CONCAT('%', goodsInfo, '%');

INSERT INTO tempGoodsInfos
(
	goods_id,
    brief_s_code,
	brand_name,
    is_common
)
SELECT distinct
	goods.goods_id,
	goods.brief_s_code, 
	goods.brand_name,
    goods.is_common
FROM ecs_goods AS goods
JOIN ecs_goods_cat AS gc
    ON gc.goods_id = goods.goods_id
JOIN categoryList AS cat
    ON cat.cat_id = gc.cat_id
WHERE goods.is_delete = 0
	AND goods.is_on_sale = 1
	AND goods.is_alone_sale = 1 
    AND IF(filteredCatId > 0, gc.cat_id = filteredCatId, true)
    AND IF(filteredGoodsBrandId > 0, goods.brand_id = filteredGoodsBrandId, true)
UNION
SELECT distinct
	goods.goods_id,
	goods.brief_s_code, 
	goods.brand_name,
    goods.is_common
FROM ecs_goods AS goods
JOIN ecs_goods_cat AS gc
    ON gc.goods_id = goods.goods_id
WHERE goods.is_delete = 0
	AND goods.is_on_sale = 1
	AND goods.is_alone_sale = 1 
    AND (goods.goods_name LIKE CONCAT('%', goodsInfo, '%') OR goods.brief_s_code LIKE CONCAT('%', goodsInfo, '%'))
    AND IF(filteredCatId > 0, gc.cat_id = filteredCatId, true)
    AND IF(filteredGoodsBrandId > 0, goods.brand_id = filteredGoodsBrandId, true);

IF filteredVehicleId = 0 AND currentVehicleId > 0 THEN
    SELECT
        count(*) INTO @c
    FROM tempGoodsInfos AS goods
    LEFT JOIN skb_goods_to_vehicles AS mapping
        ON goods.brand_name = mapping.goods_brand_name
        AND goods.brief_s_code = mapping.goods_code
        AND mapping.is_deleted = 0
    WHERE (mapping.skb_vehicle_id IS NOT NULL AND mapping.skb_vehicle_id = currentVehicleId) OR goods.is_common = 1;
    
    IF @c > 0 THEN
        SET filteredVehicleId = currentVehicleId;
    END IF;
END IF;

INSERT INTO goodsInfos
(
	goods_id
)
SELECT
    DISTINCT goods_id
FROM tempGoodsInfos AS t    
LEFT JOIN skb_goods_to_vehicles AS m
    ON t.brand_name = m.goods_brand_name
    AND t.brief_s_code = m.goods_code
    AND m.is_deleted = 0
WHERE IF(filteredVehicleId > 0, (m.skb_vehicle_id IS NOT NULL AND m.skb_vehicle_id = filteredVehicleId) OR t.is_common = 1, TRUE);

SELECT
	COUNT(DISTINCT a.cat_id) INTO @catCount
FROM goodsInfos AS gi
LEFT JOIN ecs_goods_attr AS ga 
    ON gi.goods_id = ga.goods_id
LEFT JOIN ecs_attribute AS a
    ON ga.attr_id = a.attr_id;

IF @catCount = 1 THEN

    CREATE TEMPORARY TABLE attrFilter
    (
        attr_id int,
        attr_value varchar(100)
    );
    
    IF filteredAttributes <> '' THEN
    
        SET SQL_SAFE_UPDATES = 0;
        
        CREATE TEMPORARY TABLE goodsFilteredByAttr
        (
            goods_id int
        );
        
        SET @insertFilterCmd = CONCAT('INSERT INTO attrFilter VALUES ', filteredAttributes);
        PREPARE iCmd FROM @insertFilterCmd;
        EXECUTE iCmd;
        
        SELECT
            COUNT(*) INTO @attrCount
        FROM attrFilter;
        
        INSERT INTO goodsFilteredByAttr
        (
            goods_id
        )
        SELECT 
            gi.goods_id    
        FROM goodsInfos AS gi
        JOIN ecs_goods_attr AS ga
            ON gi.goods_id = ga.goods_id
        JOIN attrFilter AS af
            ON af.attr_id = ga.attr_id
            AND af.attr_value = ga.attr_value
        GROUP BY gi.goods_id
        HAVING COUNT(*) = @attrCount;
        
        -- TODO:
        SET @@sql_safe_updates = 0;
        DELETE FROM goodsInfos
        WHERE goods_id NOT IN (
            SELECT goods_id FROM goodsFilteredByAttr
        );
        
        DROP TEMPORARY TABLE goodsFilteredByAttr;
    
    END IF;

    DROP TEMPORARY TABLE attrFilter;

END IF;

SELECT
    COUNT(goods_id)  AS goodsCount 
FROM goodsInfos;

DROP TEMPORARY TABLE categoryList;
DROP TEMPORARY TABLE tempGoodsInfos;
DROP TEMPORARY TABLE attributeInfos;
DROP TEMPORARY TABLE goodsInfos;

END$$