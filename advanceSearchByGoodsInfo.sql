DROP PROCEDURE IF EXISTS `advanceSearchByGoodsInfo`$$

CREATE PROCEDURE `advanceSearchByGoodsInfo`(
    goodsInfo varchar(200),
    filteredCatId bigint,
    filteredGoodsBrandId int,
    filteredVehicleId int,
    currentVehicleId int,
    filteredAttributes varchar(200),
    pageIndex int,
    countPerPage int,
    sortBy varchar(45),
    sortOrder varchar(10))
BEGIN

CREATE TEMPORARY TABLE categoryInfos
(
    cat_id int,
    cat_name nvarchar(90)
);

CREATE TEMPORARY TABLE vehicleInfos
(
    brand_id int,
    brand_name nvarchar(45),
    vehicle_id int,
    model_name nvarchar(100)
);

CREATE TEMPORARY TABLE goodsBrandInfos
(
    brand_id int,
    brand_name nvarchar(100)
);

CREATE TEMPORARY TABLE attributeInfos
(
    attr_id int,
    attr_name nvarchar(50),
    attr_value nvarchar(100)
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
	goods_id bigint,
    goods_sn varchar(60),
    brief_s_code varchar(120),
    brand_id int,
    brand_name nvarchar(100),
    goods_name nvarchar(100),
    goods_thumb nvarchar(255),
    goods_img nvarchar(255),
    goods_brief nvarchar(255),
    goods_type int,
    shop_price decimal(10,2) unsigned,
    market_price decimal(10,2) unsigned,
    promote_price decimal(10,2) unsigned,
    promote_start_date INT,
    promote_end_date INT,
    is_new tinyint,
    is_best tinyint,
    is_hot tinyint
);

IF pageIndex < 0 THEN
    SET pageIndex = 1;
END IF;

IF countPerPage < 0 THEN
    SET countPerPage = 12;
END IF;

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

-- Filter by current vehicle id or filtered vehicle id.
INSERT INTO goodsInfos
(
    goods_id,
    goods_sn,
    brief_s_code,
    brand_id,
    brand_name,
    goods_name,
    goods_thumb,
    goods_img,
    goods_brief,
    goods_type,
    shop_price,
    market_price,
    promote_price,
    promote_start_date,
    promote_end_date,
    is_new,
    is_best,
    is_hot
)
SELECT distinct
    goods.goods_id,
    goods.goods_sn,
    goods.brief_s_code,
    goods.brand_id,
    goods.brand_name,
    goods.goods_name,
    goods.goods_thumb,
    goods.goods_img,
    goods.goods_brief,
    goods.goods_type,
    goods.shop_price,
    goods.market_price,
    goods.promote_price,
    goods.promote_start_date,
    goods.promote_end_date,
    goods.is_new,
    goods.is_best,
    goods.is_hot
FROM ecs_goods AS goods
JOIN(
    SELECT DISTINCT
        goods_id
    FROM tempGoodsInfos AS t
    LEFT JOIN skb_goods_to_vehicles AS m
        ON t.brand_name = m.goods_brand_name
        AND t.brief_s_code = m.goods_code
        AND m.is_deleted = 0
    WHERE IF(filteredVehicleId > 0, (m.skb_vehicle_id IS NOT NULL AND m.skb_vehicle_id = filteredVehicleId) OR t.is_common = 1, TRUE)
) AS f ON f.goods_id = goods.goods_id;

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
    
        -- TODO:
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
    
    INSERT INTO attributeInfos
    (
        attr_id,
        attr_name,
        attr_value
    )
    SELECT
        a.attr_id,
        a.attr_name,
        t.attr_value
    FROM ecs_attribute AS a
    JOIN(
        SELECT
            attr_id,
            attr_value
        FROM attrFilter
        UNION
        SELECT
            attr_id,
            attr_value
        FROM ecs_goods_attr as ga
        JOIN goodsInfos as gi
            ON ga.goods_id = gi.goods_id
    ) AS t ON a.attr_id = t.attr_id;
    
    DROP TEMPORARY TABLE attrFilter;

END IF;

INSERT INTO categoryInfos (
    cat_id,
    cat_name
)
SELECT
    c.cat_id,
    c.cat_name
FROM ecs_category AS c
JOIN (
    SELECT
        filteredCatId AS cat_id
    UNION
    SELECT DISTINCT
        gc.cat_id
    FROM goodsInfos AS g
    JOIN ecs_goods_cat AS gc
        ON g.goods_id = gc.goods_id
) AS f ON f.cat_id = c.cat_id;

IF filteredVehicleId > 0 THEN
    -- VehicleInfos contains only the filtered vehicle info.
    INSERT INTO vehicleInfos
    (
        vehicle_id,
        model_name
    )
    SELECT
        vehicle_id,
        model_name
    FROM skb_vehicles AS vehicle
    WHERE vehicle_id = filteredVehicleId;
ELSE
    -- VehicleInfos contains optional vehicle brands info.
    INSERT INTO vehicleInfos
    (
        brand_id,
        brand_name
    )
    SELECT DISTINCT
        brand.brand_id,
        brand.brand_name
    FROM skb_vehicles AS vehicle
    JOIN (
        SELECT DISTINCT
            mapping.skb_vehicle_id AS vehicle_id
        FROM skb_goods_to_vehicles AS mapping
        JOIN goodsInfos AS g
            ON g.brand_name = mapping.goods_brand_name
            AND g.brief_s_code = mapping.goods_code
        WHERE mapping.is_deleted = 0
    ) AS s ON s.vehicle_id = vehicle.vehicle_id
    JOIN skb_vehicle_brands AS brand
        ON brand.brand_id = vehicle.brand_id
    WHERE vehicle.is_deleted = 0;
END IF;

INSERT INTO goodsBrandInfos
(
    brand_id,
    brand_name
)
SELECT DISTINCT
    brand_id,
    brand_name
FROM goodsInfos;

-- ResultSet 0: categories shown in filters.
SELECT
    cat_id,
    cat_name
FROM categoryInfos
ORDER BY cat_name;

-- ResultSet 1: vehicles shown in filters.
SELECT
    brand_id,
    brand_name,
    vehicle_id,
    model_name
FROM vehicleInfos;

-- ResultSet 2: goods brands shown in filters.
SELECT
    brand_id,
    brand_name
FROM goodsBrandInfos;

-- ResultSet 3: attributes if all goods are the same goods type.
SELECT
    attr_id,
    attr_name,
    attr_value
FROM attributeInfos
ORDER BY attr_id, attr_value;

SET @selectString =
'SELECT DISTINCT
    goodsInfo.goods_id,
    goodsInfo.goods_sn,
    goodsInfo.brief_s_code,
    goodsInfo.brand_id,
    goodsInfo.brand_name,
    goodsInfo.goods_name,
    goodsInfo.goods_thumb,
    goodsInfo.goods_img,
    goodsInfo.goods_brief,
    goodsInfo.goods_type,
    goodsInfo.shop_price,
    goodsInfo.market_price,
    goodsInfo.promote_price,
    goodsInfo.promote_start_date,
    goodsInfo.promote_end_date,
    goodsInfo.is_new,
    goodsInfo.is_best,
    goodsInfo.is_hot
FROM goodsInfos AS goodsInfo';

SET @joinString_sortBySales = IF(
    sortBy = 'sales_volume',
    "join skb_ecs_goods as e on goodsInfo.goods_id = e.ecs_goodsId",
    '');

SET @orderByString = concat('order by ', IF(sortBy = 'sales_volume', 'sales', sortBy), ' ', sortOrder);
SET @limitString = concat('limit ', ceil((pageIndex -1) * countPerPage), ',',ceil(countPerPage));

SET @cmd = concat(
    @selectString,
    ' ',
    @joinString_sortBySales,
    ' ',
    @orderByString,
    ' ',
    @limitString);

PREPARE s FROM @cmd;

-- ResultSet 4: filtered goods.
EXECUTE s;

DROP TEMPORARY TABLE categoryInfos;
DROP TEMPORARY TABLE vehicleInfos;
DROP TEMPORARY TABLE goodsBrandInfos;
DROP TEMPORARY TABLE attributeInfos;
DROP TEMPORARY TABLE tempGoodsInfos;
DROP TEMPORARY TABLE goodsInfos;

END$$