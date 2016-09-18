DROP PROCEDURE IF EXISTS `advanceSearchByGoodsInfoVehicleFilter`$$

CREATE PROCEDURE `advanceSearchByGoodsInfoVehicleFilter`(
    goodsInfo varchar(200),
    filteredCatId bigint,
    filteredGoodsBrandId int,
    filteredAttributes varchar(200),
    vehicleBrandId int,
    vehicleSerieId int,
    vehicleManufactoryId int,
    vehicleModelYear nvarchar(45)
)
BEGIN

CREATE TEMPORARY TABLE tempVehicleIds
(
    vehicle_id int
);

CREATE TEMPORARY TABLE tempGoodsInfos
(
    goods_id bigint,
    brief_s_code varchar(120), 
    brand_name nvarchar(100)
);

CREATE TEMPORARY TABLE goodsInfos
(
	goods_id bigint,
	brief_s_code varchar(120), 
	brand_name nvarchar(100)
);

CREATE TEMPORARY TABLE attributeInfos
(
    attr_id int,
    attr_name nvarchar(50),
    attr_value nvarchar(100)
);

INSERT INTO tempGoodsInfos
(
    goods_id,
    brief_s_code,
    brand_name
)
SELECT distinct
    goods.goods_id,
    goods.brief_s_code, 
    goods.brand_name
FROM ecs_goods AS goods
JOIN ecs_goods_cat AS gc
    ON gc.goods_id = goods.goods_id
JOIN (
    -- Filter by advance search input.
    SELECT
        cat.cat_id
    FROM ecs_category as cat
    WHERE cat.cat_name LIKE CONCAT('%', goodsInfo, '%')
    UNION
    SELECT
        alias.cat_id
    FROM skb_vehicle_accessory_alias AS alias
    WHERE alias.accessory_alias_value LIKE CONCAT('%', goodsInfo, '%')
) AS cat ON cat.cat_id = gc.cat_id
WHERE goods.is_common = 0
    AND goods.is_delete = 0
    AND goods.is_on_sale = 1
    AND goods.is_alone_sale = 1 
    AND IF(filteredCatId > 0, gc.cat_id = filteredCatId, true)
    AND IF(filteredGoodsBrandId > 0, goods.brand_id = filteredGoodsBrandId, true)
UNION
SELECT distinct
	goods.goods_id,
	goods.brief_s_code, 
	goods.brand_name
FROM ecs_goods AS goods
JOIN ecs_goods_cat AS gc
    ON gc.goods_id = goods.goods_id
WHERE goods.is_common = 0
    AND goods.is_delete = 0
	AND goods.is_on_sale = 1
	AND goods.is_alone_sale = 1 
    AND (goods.goods_name LIKE CONCAT('%', goodsInfo, '%') OR goods.brief_s_code LIKE CONCAT('%', goodsInfo, '%'))
    AND IF(filteredCatId > 0, gc.cat_id = filteredCatId, true)
    AND IF(filteredGoodsBrandId > 0, goods.brand_id = filteredGoodsBrandId, true);

SELECT
	COUNT(DISTINCT a.cat_id) INTO @catCount
FROM tempGoodsInfos AS gi
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
        FROM tempGoodsInfos AS gi
        JOIN ecs_goods_attr AS ga
            ON gi.goods_id = ga.goods_id
        JOIN attrFilter AS af
            ON af.attr_id = ga.attr_id
            AND af.attr_value = ga.attr_value
        GROUP BY gi.goods_id
        HAVING COUNT(*) = @attrCount;
        
        INSERT INTO goodsInfos (
            goods_id,
            brief_s_code,
            brand_name
        )
        SELECT
            goods_id,
            brief_s_code,
            brand_name
        FROM tempGoodsInfos
        WHERE goods_id IN (
            SELECT goods_id FROM goodsFilteredByAttr
        );
        
        DROP TEMPORARY TABLE goodsFilteredByAttr;
    
    END IF;

    DROP TEMPORARY TABLE attrFilter;

END IF;

INSERT INTO tempVehicleIds
SELECT DISTINCT
    m.skb_vehicle_id
FROM goodsInfos AS t    
JOIN skb_goods_to_vehicles AS m
    ON t.brand_name = m.goods_brand_name
    AND t.brief_s_code = m.goods_code
    AND m.is_deleted = 0;

IF vehicleBrandId > 0 AND vehicleSerieId > 0 THEN
    IF vehicleManufactoryId > 0 THEN
        IF vehicleModelYear <> '' AND vehicleModelYear <> '0' THEN
            -- vehicle_id => model_name
            SELECT DISTINCT
                vehicle.vehicle_id AS id,
                vehicle.model_name AS name
            FROM skb_vehicles AS vehicle
            JOIN tempVehicleIds AS temp
                ON vehicle.vehicle_id = temp.vehicle_id
            WHERE vehicle.brand_id = vehicleBrandId
                AND vehicle.serie_id = vehicleSerieId
                AND vehicle.manufactory_id = vehicleManufactoryId
                AND vehicle.model_year = vehicleModelYear
            ORDER BY name;
            -- model_year => model_year
        ELSE
            SELECT DISTINCT
                vehicle.model_year AS id,
                vehicle.model_year AS name
            FROM skb_vehicles AS vehicle
            JOIN tempVehicleIds AS temp
                ON vehicle.vehicle_id = temp.vehicle_id
            WHERE vehicle.brand_id = vehicleBrandId
                AND vehicle.serie_id = vehicleSerieId
                AND vehicle.manufactory_id = vehicleManufactoryId
            ORDER BY name;
        END IF;
    ELSE
        -- manufactory_id => manufactory_name
        SELECT DISTINCT
            vehicle.manufactory_id AS id,
            manu.manufactory_name AS name
        FROM skb_vehicles AS vehicle
        JOIN tempVehicleIds AS temp
            ON vehicle.vehicle_id = temp.vehicle_id
        JOIN skb_vehicle_manufactories AS manu
            ON manu.manufactory_id = vehicle.manufactory_id
        WHERE vehicle.brand_id = vehicleBrandId
            AND vehicle.serie_id = vehicleSerieId
        ORDER BY name;
    END IF;
ELSE
    -- serie_id => serie_name
    SELECT DISTINCT
        vehicle.serie_id AS id,
        serie.serie_name AS name
    FROM skb_vehicles AS vehicle
    JOIN tempVehicleIds AS temp
        ON vehicle.vehicle_id = temp.vehicle_id
    JOIN skb_vehicle_series AS serie
        ON serie.serie_id = vehicle.serie_id
    WHERE vehicle.brand_id = vehicleBrandId
    ORDER BY name;
END IF;

DROP TEMPORARY TABLE tempVehicleIds;
DROP TEMPORARY TABLE tempGoodsInfos;
DROP TEMPORARY TABLE goodsInfos;
DROP TEMPORARY TABLE attributeInfos;

END$$