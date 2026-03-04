CREATE OR REPLACE PROCEDURE TABULARIUM.GOLD.BI_MKT_TREND_SALES_BY_DSO_SUB_TEST(
    "GROUP_PRINCIPAL_ID" NUMBER(38, 0) DEFAULT NULL,
    "P_ADJUSTMENT_WEEK"  NUMBER(38, 0) DEFAULT -1
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
/*
  ============================================================
  Author               : Dwicky
  Creation Date        : 18-02-2026
  Description          : Get Data Trend Sales By DSO Sub
  ============================================================
  Parameters:
    GROUP_PRINCIPAL_ID  I/O: -  Type: Number  Example: 1, NULL   Set Group Principal ID
    P_ADJUSTMENT_WEEK   I/O: -  Type: Number  Example: -1, -2    Set how many weeks back (default: prev week)
  ============================================================
  Example:
    CALL TABULARIUM.GOLD.BI_MKT_TREND_SALES_BY_DSO_SUB(1, -2);  -- Process GP 1, Minus 2 weeks
  ============================================================
  Modification History:
    ID  Chg-ReqNo  Date        User  Description
    1   -          18-02-2026  DFP   Creation for the first time
    2   -          02-03-2026  DFP   Update satuan Juta BTG ke Bal
    3   -          03-03-2026  DFP   Add INVENTORY, SALES_MIN, SALES_MAX metrics
    4   -          04-03-2026  DFP   Fix #1: CUR join DB corrected SILKROAD->TABULARIUM
                                     Fix #3: Conversion cols removed from GROUP BY (fan-out)
                                     Fix #6: GROUP_PRINCIPAL_ID filter pushed into FAKTUR_SUM
                                     Impl B: QUALIFY ROW_NUMBER() dedup on all conversion joins
                                     Impl C: GROUP_PRINCIPAL_ID early filter (scan reduction)
  ============================================================
*/
BEGIN

    IF (GROUP_PRINCIPAL_ID IS NULL OR GROUP_PRINCIPAL_ID IN (1, 3)) THEN

        MERGE INTO TABULARIUM.GOLD.MKT_TREND_SALES_BY_DSO_SUB AS tgt
        USING (
            WITH
            -- ================================================================
            -- CTE 1: Resolve current week and prior-year comparison week
            -- ================================================================
            CTE_GET_DATE AS (
                SELECT
                    MAX(CASE WHEN CURRENT_DATE - (7 - (7 * (:P_ADJUSTMENT_WEEK + 1)))        BETWEEN START_DATE AND END_DATE THEN TAHUN  END) AS THIS_TAHUN,
                    MAX(CASE WHEN CURRENT_DATE - (7 - (7 * (:P_ADJUSTMENT_WEEK + 1)))        BETWEEN START_DATE AND END_DATE THEN MINGGU END) AS THIS_MINGGU,
                    MAX(CASE WHEN CURRENT_DATE - ((7 * 54) - (7 * (:P_ADJUSTMENT_WEEK + 1))) BETWEEN START_DATE AND END_DATE THEN TAHUN  END) AS BEFORE_TAHUN,
                    MAX(CASE WHEN CURRENT_DATE - ((7 * 54) - (7 * (:P_ADJUSTMENT_WEEK + 1))) BETWEEN START_DATE AND END_DATE THEN MINGGU END) AS BEFORE_MINGGU
                FROM TABULARIUM.BRONZE.TBLSAM_PERIODE_MINGGU
            ),
            -- ================================================================
            -- CTE 2: Aggregate sales quantities with UOM conversion
            -- ================================================================
            FAKTUR_SUM AS (
                SELECT
                    DSO_SUB.SOURCE_DB,
                    DSO_SUB.TAHUN,
                    DSO_SUB.MINGGU,
                    DSO_SUB.COMPANY,
                    DSO_SUB.RSO,
                    DSO_SUB.AREA,
                    DSO_SUB.DSO_SUB,
                    DSO_SUB.DSO_CATEGORY,
                    DSO_SUB.PRODUCT_ID,
                    DSO_SUB.PRODUCT_LINE_ID,
                    DSO_SUB.UOM_INVENTORY_ID,
                    DSO_SUB.UOM_SALES_MIN_ID,
                    DSO_SUB.UOM_SALES_MAX_ID,
                    DSO_SUB.GROUP_PRINCIPAL_ID,
                    SUM(DSO_SUB.QUANTITY_INVENTORY) AS QUANTITY_INVENTORY,
                    ROUND(
                        SUM(
                            CAST(CASE
                                WHEN CUR.SEQUENCE IS NULL OR TAR_MIN.SEQUENCE IS NULL THEN
                                    CAST(DSO_SUB.QUANTITY_INVENTORY AS NUMBER(38, 10))
                                ELSE
                                    CAST(DSO_SUB.QUANTITY_INVENTORY AS NUMBER(38, 10))
                                    * CAST(
                                        EXP(
                                            CASE
                                                WHEN CUR.SEQUENCE = TAR_MIN.SEQUENCE THEN 0
                                                WHEN CUR.SEQUENCE < TAR_MIN.SEQUENCE THEN TAR_MIN.CUM_LN - CUR.PREV_CUM_LN
                                                ELSE                                       TAR_MIN.CUM_LN - CUR.CUM_LN
                                            END
                                        ) AS NUMBER(38, 10)
                                    )
                            END AS NUMBER(38, 3))
                        ), 3
                    ) AS QUANTITY_SALES_MIN,
                    ROUND(
                        SUM(
                            CAST(CASE
                                WHEN CUR.SEQUENCE IS NULL OR TAR_MAX.SEQUENCE IS NULL THEN
                                    CAST(DSO_SUB.QUANTITY_INVENTORY AS NUMBER(38, 10))
                                ELSE
                                    CAST(DSO_SUB.QUANTITY_INVENTORY AS NUMBER(38, 10))
                                    * CAST(
                                        EXP(
                                            CASE
                                                WHEN CUR.SEQUENCE = TAR_MAX.SEQUENCE THEN 0
                                                WHEN CUR.SEQUENCE < TAR_MAX.SEQUENCE THEN TAR_MAX.CUM_LN - CUR.PREV_CUM_LN
                                                ELSE                                       TAR_MAX.CUM_LN - CUR.CUM_LN
                                            END
                                        ) AS NUMBER(38, 10)
                                    )
                            END AS NUMBER(38, 3))
                        ), 3
                    ) AS QUANTITY_SALES_MAX
                FROM  TABULARIUM.SILVER.MKT_FAKTUR_SUM_BY_DSO_SUB  DSO_SUB
                JOIN  TABULARIUM.BRONZE.TBLMDM_PRODUCT              PRODUCT
                      ON  PRODUCT.PRODUCT_ID         = DSO_SUB.PRODUCT_ID
                -- Fix #1: corrected from SILKROAD to TABULARIUM
                -- Approach B: QUALIFY deduplicates to one row per (CONVERSION_RULE_ID, UOM_ID)
                -- so conversion cols no longer needed in GROUP BY (Fix #3)
                LEFT JOIN (
                    SELECT CONVERSION_RULE_ID, UOM_ID, SEQUENCE, PREV_CUM_LN, CUM_LN
                    FROM   TABULARIUM.SILVER.MKT_CONVERSION_DETAIL_CUMLN
                    WHERE  IS_ACTIVE = TRUE
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY CONVERSION_RULE_ID, UOM_ID ORDER BY SEQUENCE) = 1
                ) AS CUR
                      ON  CUR.CONVERSION_RULE_ID     = PRODUCT.CONVERSION_RULE_ID
                      AND CUR.UOM_ID                 = DSO_SUB.UOM_INVENTORY_ID
                LEFT JOIN (
                    SELECT CONVERSION_RULE_ID, UOM_ID, SEQUENCE, CUM_LN
                    FROM   TABULARIUM.SILVER.MKT_CONVERSION_DETAIL_CUMLN
                    WHERE  IS_ACTIVE = TRUE
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY CONVERSION_RULE_ID, UOM_ID ORDER BY SEQUENCE) = 1
                ) AS TAR_MIN
                      ON  TAR_MIN.CONVERSION_RULE_ID = PRODUCT.CONVERSION_RULE_ID
                      AND TAR_MIN.UOM_ID             = PRODUCT.UOM_SALES_MIN_ID
                LEFT JOIN (
                    SELECT CONVERSION_RULE_ID, UOM_ID, SEQUENCE, CUM_LN
                    FROM   TABULARIUM.SILVER.MKT_CONVERSION_DETAIL_CUMLN
                    WHERE  IS_ACTIVE = TRUE
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY CONVERSION_RULE_ID, UOM_ID ORDER BY SEQUENCE) = 1
                ) AS TAR_MAX
                      ON  TAR_MAX.CONVERSION_RULE_ID = PRODUCT.CONVERSION_RULE_ID
                      AND TAR_MAX.UOM_ID             = PRODUCT.UOM_SALES_MAX_ID
                -- Limit scan to 25 months of history
                -- Fix #6 / Approach C: GROUP_PRINCIPAL_ID pushed early to reduce scanned rows
                WHERE DSO_SUB.TAHUN >= YEAR(DATEADD(''MONTH'', -25, CURRENT_DATE))
                  AND (
                          DSO_SUB.TAHUN > YEAR(DATEADD(''MONTH'', -25, CURRENT_DATE))
                       OR DSO_SUB.MINGGU >= (
                              SELECT MIN(MINGGU)
                              FROM   TABULARIUM.BRONZE.TBLSAM_PERIODE_MINGGU
                              WHERE  DATEADD(''MONTH'', -25, CURRENT_DATE) BETWEEN START_DATE AND END_DATE
                          )
                  )
                  AND (:GROUP_PRINCIPAL_ID IS NULL OR DSO_SUB.GROUP_PRINCIPAL_ID = :GROUP_PRINCIPAL_ID)
                GROUP BY
                    DSO_SUB.SOURCE_DB,
                    DSO_SUB.TAHUN,
                    DSO_SUB.MINGGU,
                    DSO_SUB.COMPANY,
                    DSO_SUB.RSO,
                    DSO_SUB.AREA,
                    DSO_SUB.DSO_SUB,
                    DSO_SUB.DSO_CATEGORY,
                    DSO_SUB.PRODUCT_ID,
                    DSO_SUB.PRODUCT_LINE_ID,
                    DSO_SUB.UOM_INVENTORY_ID,
                    DSO_SUB.UOM_SALES_MIN_ID,
                    DSO_SUB.UOM_SALES_MAX_ID,
                    DSO_SUB.GROUP_PRINCIPAL_ID
            ),
            -- ================================================================
            -- CTE 3: Single-pass aggregation across all metric groups
            -- ================================================================
            AGGREGATED_DATA AS (
                SELECT
                    -- ── Dimension Keys ──────────────────────────────────────
                    m.RSO,
                    m.AREA,
                    m.DSO_SUB,
                    m.PRODUCT_ID,
                    m.COMPANY,
                    cprincipal.company_principal AS COMPANY_PRINCIPAL,
                    d.THIS_TAHUN,
                    d.THIS_MINGGU,

                    -- ── [1] JT BTG  (QUANTITY_INVENTORY / 2000 / 3000) ─────
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.THIS_MINGGU   THEN m.QUANTITY_INVENTORY ELSE 0 END), 0) / 2000 / 3000 AS THIS_WEEK_RAW,
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.BEFORE_MINGGU THEN m.QUANTITY_INVENTORY ELSE 0 END), 0) / 2000 / 3000 AS LAST_WEEK_RAW,
                    SUM(IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0)
                        / 2000 / 3000 AS AVG_CTM_RAW,
                    SUM(IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0)
                        / 2000 / 3000 AS AVG_PTM_RAW,
                    SUM(IFF(m.TAHUN = (d.THIS_TAHUN - 1), COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = (d.THIS_TAHUN - 1), m.MINGGU, NULL)), 0)
                        / 2000 / 3000 AS AVG_PFY_RAW,

                    -- ── [2] INVENTORY  (raw QUANTITY_INVENTORY, no divisor) ─
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.THIS_MINGGU   THEN m.QUANTITY_INVENTORY ELSE 0 END), 0) AS INV_THIS_WEEK_RAW,
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.BEFORE_MINGGU THEN m.QUANTITY_INVENTORY ELSE 0 END), 0) AS INV_LAST_WEEK_RAW,
                    SUM(IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS INV_AVG_CTM_RAW,
                    SUM(IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS INV_AVG_PTM_RAW,
                    SUM(IFF(m.TAHUN = (d.THIS_TAHUN - 1), COALESCE(m.QUANTITY_INVENTORY, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = (d.THIS_TAHUN - 1), m.MINGGU, NULL)), 0) AS INV_AVG_PFY_RAW,

                    -- ── [3] SALES MIN ────────────────────────────────────────
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.THIS_MINGGU   THEN m.QUANTITY_SALES_MIN ELSE 0 END), 0) AS MIN_THIS_WEEK_RAW,
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.BEFORE_MINGGU THEN m.QUANTITY_SALES_MIN ELSE 0 END), 0) AS MIN_LAST_WEEK_RAW,
                    SUM(IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_SALES_MIN, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS MIN_AVG_CTM_RAW,
                    SUM(IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_SALES_MIN, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS MIN_AVG_PTM_RAW,
                    SUM(IFF(m.TAHUN = (d.THIS_TAHUN - 1), COALESCE(m.QUANTITY_SALES_MIN, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = (d.THIS_TAHUN - 1), m.MINGGU, NULL)), 0) AS MIN_AVG_PFY_RAW,

                    -- ── [4] SALES MAX ────────────────────────────────────────
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.THIS_MINGGU   THEN m.QUANTITY_SALES_MAX ELSE 0 END), 0) AS MAX_THIS_WEEK_RAW,
                    COALESCE(SUM(CASE WHEN m.TAHUN = d.THIS_TAHUN   AND m.MINGGU = d.BEFORE_MINGGU THEN m.QUANTITY_SALES_MAX ELSE 0 END), 0) AS MAX_LAST_WEEK_RAW,
                    SUM(IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_SALES_MAX, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.THIS_TAHUN    AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS MAX_AVG_CTM_RAW,
                    SUM(IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, COALESCE(m.QUANTITY_SALES_MAX, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = d.BEFORE_TAHUN  AND m.MINGGU >= 1 AND m.MINGGU <= d.THIS_MINGGU, m.MINGGU, NULL)), 0) AS MAX_AVG_PTM_RAW,
                    SUM(IFF(m.TAHUN = (d.THIS_TAHUN - 1), COALESCE(m.QUANTITY_SALES_MAX, 0), 0))
                        / NULLIF(COUNT(DISTINCT IFF(m.TAHUN = (d.THIS_TAHUN - 1), m.MINGGU, NULL)), 0) AS MAX_AVG_PFY_RAW

                FROM  FAKTUR_SUM m
                JOIN  TABULARIUM.GOLD.MKT_LOMN_COMPANY_PRINCIPAL cprincipal ON cprincipal.company = m.company
                CROSS JOIN CTE_GET_DATE d
                WHERE m.TAHUN IN (d.THIS_TAHUN, d.BEFORE_TAHUN, d.THIS_TAHUN - 1)
                  AND (:GROUP_PRINCIPAL_ID IS NULL OR m.GROUP_PRINCIPAL_ID = :GROUP_PRINCIPAL_ID)
                GROUP BY
                    m.RSO,
                    m.AREA,
                    m.DSO_SUB,
                    m.PRODUCT_ID,
                    m.COMPANY,
                    cprincipal.company_principal,
                    d.THIS_TAHUN,
                    d.THIS_MINGGU,
                    d.BEFORE_TAHUN,
                    d.BEFORE_MINGGU
            )
            -- ================================================================
            -- Final projection: compute deltas, pct changes and directions
            -- ================================================================
            SELECT
                -- ── Dimension Keys ──────────────────────────────────────────
                AGG.THIS_TAHUN          AS CUR_TAHUN,
                AGG.THIS_MINGGU         AS CUR_MINGGU,
                AGG.RSO,
                AGG.AREA,
                AGG.DSO_SUB,
                AGG.PRODUCT_ID,
                AGG.COMPANY,
                AGG.COMPANY_PRINCIPAL,

                -- ── [1] JT BTG ───────────────────────────────────────────────
                -- Current week vs Before-current-week
                ROUND(AGG.THIS_WEEK_RAW, 15)                                                                                            AS CTCM_QTY_JT_BTG,
                ROUND(AGG.LAST_WEEK_RAW, 15)                                                                                            AS BCTCM_QTY_JT_BTG,
                COALESCE(ROUND(AGG.THIS_WEEK_RAW - AGG.LAST_WEEK_RAW, 15), 0)                                                          AS CTCM_VS_BCTCM_CHANGE,
                COALESCE(ROUND(((AGG.THIS_WEEK_RAW / NULLIF(AGG.LAST_WEEK_RAW, 0)) - 1) * 100, 15), 0)                                 AS CTCM_VS_BCTCM_CHANGE_PCT,
                CASE WHEN (AGG.THIS_WEEK_RAW - AGG.LAST_WEEK_RAW) = 0 THEN ''FLAT''
                     WHEN (AGG.THIS_WEEK_RAW - AGG.LAST_WEEK_RAW) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS CTCM_VS_BCTCM_DIRECTION,
                -- Average current-to-month vs prior-to-month
                ROUND(COALESCE(AGG.AVG_CTM_RAW, 0), 15)                                                                                AS AVG_CTCM_QTY_JT_BTG,
                ROUND(COALESCE(AGG.AVG_PTM_RAW, 0), 15)                                                                                AS AVG_PTCM_QTY_JT_BTG,
                ROUND(COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PTM_RAW, 0), 15)                                                AS AVG_CTCM_VS_AVG_PTCM_CHANGE,
                CASE WHEN COALESCE(AGG.AVG_PTM_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.AVG_CTM_RAW, 0) / AGG.AVG_PTM_RAW) - 1) * 100, 15) END                                 AS AVG_CTCM_VS_AVG_PTCM_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PTM_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PTM_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_PTCM_DIRECTION,
                -- Average current-to-month vs last-year full-year
                ROUND(COALESCE(AGG.AVG_PFY_RAW, 0), 15)                                                                                AS AVG_LYFY_QTY_JT_BTG,
                ROUND(COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PFY_RAW, 0), 15)                                                AS AVG_CTCM_VS_AVG_LYFY_CHANGE,
                CASE WHEN COALESCE(AGG.AVG_PFY_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.AVG_CTM_RAW, 0) / AGG.AVG_PFY_RAW) - 1) * 100, 15) END                                 AS AVG_CTCM_VS_AVG_LYFY_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PFY_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.AVG_CTM_RAW, 0) - COALESCE(AGG.AVG_PFY_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_LYFY_DIRECTION,

                -- ── [2] INVENTORY ────────────────────────────────────────────
                -- Current week vs Before-current-week
                ROUND(AGG.INV_THIS_WEEK_RAW, 15)                                                                                        AS CTCM_QTY_INVENTORY,
                ROUND(AGG.INV_LAST_WEEK_RAW, 15)                                                                                        AS BCTCM_QTY_INVENTORY,
                COALESCE(ROUND(AGG.INV_THIS_WEEK_RAW - AGG.INV_LAST_WEEK_RAW, 15), 0)                                                  AS CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE,
                COALESCE(ROUND(((AGG.INV_THIS_WEEK_RAW / NULLIF(AGG.INV_LAST_WEEK_RAW, 0)) - 1) * 100, 15), 0)                         AS CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE_PCT,
                CASE WHEN (AGG.INV_THIS_WEEK_RAW - AGG.INV_LAST_WEEK_RAW) = 0 THEN ''FLAT''
                     WHEN (AGG.INV_THIS_WEEK_RAW - AGG.INV_LAST_WEEK_RAW) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS CTCM_VS_BCTCM_QTY_INVENTORY_DIRECTION,
                -- Average current-to-month vs prior-to-month
                ROUND(COALESCE(AGG.INV_AVG_CTM_RAW, 0), 15)                                                                            AS AVG_CTCM_QTY_INVENTORY,
                ROUND(COALESCE(AGG.INV_AVG_PTM_RAW, 0), 15)                                                                            AS AVG_PTCM_QTY_INVENTORY,
                ROUND(COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PTM_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE,
                CASE WHEN COALESCE(AGG.INV_AVG_PTM_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.INV_AVG_CTM_RAW, 0) / AGG.INV_AVG_PTM_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PTM_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PTM_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_DIRECTION,
                -- Average current-to-month vs last-year full-year
                ROUND(COALESCE(AGG.INV_AVG_PFY_RAW, 0), 15)                                                                            AS AVG_LYFY_QTY_INVENTORY,
                ROUND(COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PFY_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE,
                CASE WHEN COALESCE(AGG.INV_AVG_PFY_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.INV_AVG_CTM_RAW, 0) / AGG.INV_AVG_PFY_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PFY_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.INV_AVG_CTM_RAW, 0) - COALESCE(AGG.INV_AVG_PFY_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_DIRECTION,

                -- ── [3] SALES MIN ─────────────────────────────────────────────
                -- Current week vs Before-current-week
                ROUND(AGG.MIN_THIS_WEEK_RAW, 15)                                                                                        AS CTCM_QTY_SALES_MIN,
                ROUND(AGG.MIN_LAST_WEEK_RAW, 15)                                                                                        AS BCTCM_QTY_SALES_MIN,
                COALESCE(ROUND(AGG.MIN_THIS_WEEK_RAW - AGG.MIN_LAST_WEEK_RAW, 15), 0)                                                  AS CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE,
                COALESCE(ROUND(((AGG.MIN_THIS_WEEK_RAW / NULLIF(AGG.MIN_LAST_WEEK_RAW, 0)) - 1) * 100, 15), 0)                         AS CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE_PCT,
                CASE WHEN (AGG.MIN_THIS_WEEK_RAW - AGG.MIN_LAST_WEEK_RAW) = 0 THEN ''FLAT''
                     WHEN (AGG.MIN_THIS_WEEK_RAW - AGG.MIN_LAST_WEEK_RAW) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS CTCM_VS_BCTCM_QTY_SALES_MIN_DIRECTION,
                -- Average current-to-month vs prior-to-month
                ROUND(COALESCE(AGG.MIN_AVG_CTM_RAW, 0), 15)                                                                            AS AVG_CTCM_QTY_SALES_MIN,
                ROUND(COALESCE(AGG.MIN_AVG_PTM_RAW, 0), 15)                                                                            AS AVG_PTCM_QTY_SALES_MIN,
                ROUND(COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PTM_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE,
                CASE WHEN COALESCE(AGG.MIN_AVG_PTM_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.MIN_AVG_CTM_RAW, 0) / AGG.MIN_AVG_PTM_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PTM_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PTM_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_DIRECTION,
                -- Average current-to-month vs last-year full-year
                ROUND(COALESCE(AGG.MIN_AVG_PFY_RAW, 0), 15)                                                                            AS AVG_LYFY_QTY_SALES_MIN,
                ROUND(COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PFY_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE,
                CASE WHEN COALESCE(AGG.MIN_AVG_PFY_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.MIN_AVG_CTM_RAW, 0) / AGG.MIN_AVG_PFY_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PFY_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.MIN_AVG_CTM_RAW, 0) - COALESCE(AGG.MIN_AVG_PFY_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_DIRECTION,

                -- ── [4] SALES MAX ─────────────────────────────────────────────
                -- Current week vs Before-current-week
                ROUND(AGG.MAX_THIS_WEEK_RAW, 15)                                                                                        AS CTCM_QTY_SALES_MAX,
                ROUND(AGG.MAX_LAST_WEEK_RAW, 15)                                                                                        AS BCTCM_QTY_SALES_MAX,
                COALESCE(ROUND(AGG.MAX_THIS_WEEK_RAW - AGG.MAX_LAST_WEEK_RAW, 15), 0)                                                  AS CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE,
                COALESCE(ROUND(((AGG.MAX_THIS_WEEK_RAW / NULLIF(AGG.MAX_LAST_WEEK_RAW, 0)) - 1) * 100, 15), 0)                         AS CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE_PCT,
                CASE WHEN (AGG.MAX_THIS_WEEK_RAW - AGG.MAX_LAST_WEEK_RAW) = 0 THEN ''FLAT''
                     WHEN (AGG.MAX_THIS_WEEK_RAW - AGG.MAX_LAST_WEEK_RAW) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS CTCM_VS_BCTCM_QTY_SALES_MAX_DIRECTION,
                -- Average current-to-month vs prior-to-month
                ROUND(COALESCE(AGG.MAX_AVG_CTM_RAW, 0), 15)                                                                            AS AVG_CTCM_QTY_SALES_MAX,
                ROUND(COALESCE(AGG.MAX_AVG_PTM_RAW, 0), 15)                                                                            AS AVG_PTCM_QTY_SALES_MAX,
                ROUND(COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PTM_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE,
                CASE WHEN COALESCE(AGG.MAX_AVG_PTM_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.MAX_AVG_CTM_RAW, 0) / AGG.MAX_AVG_PTM_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PTM_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PTM_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_DIRECTION,
                -- Average current-to-month vs last-year full-year
                ROUND(COALESCE(AGG.MAX_AVG_PFY_RAW, 0), 15)                                                                            AS AVG_LYFY_QTY_SALES_MAX,
                ROUND(COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PFY_RAW, 0), 15)                                        AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE,
                CASE WHEN COALESCE(AGG.MAX_AVG_PFY_RAW, 0) = 0 THEN 0.0
                     ELSE ROUND(((COALESCE(AGG.MAX_AVG_CTM_RAW, 0) / AGG.MAX_AVG_PFY_RAW) - 1) * 100, 15) END                         AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE_PCT,
                CASE WHEN COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PFY_RAW, 0) = 0 THEN ''FLAT''
                     WHEN COALESCE(AGG.MAX_AVG_CTM_RAW, 0) - COALESCE(AGG.MAX_AVG_PFY_RAW, 0) < 0 THEN ''DECREASE''
                     ELSE ''INCREASE'' END                                                                                              AS AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_DIRECTION

            FROM AGGREGATED_DATA AGG
        ) AS src
        ON  tgt.CUR_TAHUN         = src.CUR_TAHUN
        AND tgt.CUR_MINGGU        = src.CUR_MINGGU
        AND tgt.COMPANY           = src.COMPANY
        AND tgt.RSO               = src.RSO
        AND tgt.AREA              = src.AREA
        AND tgt.DSO_SUB           = src.DSO_SUB
        AND tgt.PRODUCT_ID        = src.PRODUCT_ID
        AND tgt.COMPANY_PRINCIPAL = src.COMPANY_PRINCIPAL

        -- ── MATCHED: update all metric columns ─────────────────────────────
        WHEN MATCHED THEN UPDATE SET
            -- [1] JT BTG
            tgt.CTCM_QTY_JT_BTG                        = src.CTCM_QTY_JT_BTG,
            tgt.BCTCM_QTY_JT_BTG                       = src.BCTCM_QTY_JT_BTG,
            tgt.CTCM_VS_BCTCM_CHANGE                   = src.CTCM_VS_BCTCM_CHANGE,
            tgt.CTCM_VS_BCTCM_CHANGE_PCT               = src.CTCM_VS_BCTCM_CHANGE_PCT,
            tgt.CTCM_VS_BCTCM_DIRECTION                = src.CTCM_VS_BCTCM_DIRECTION,
            tgt.AVG_CTCM_QTY_JT_BTG                    = src.AVG_CTCM_QTY_JT_BTG,
            tgt.AVG_PTCM_QTY_JT_BTG                    = src.AVG_PTCM_QTY_JT_BTG,
            tgt.AVG_CTCM_VS_AVG_PTCM_CHANGE            = src.AVG_CTCM_VS_AVG_PTCM_CHANGE,
            tgt.AVG_CTCM_VS_AVG_PTCM_CHANGE_PCT        = src.AVG_CTCM_VS_AVG_PTCM_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_PTCM_DIRECTION         = src.AVG_CTCM_VS_AVG_PTCM_DIRECTION,
            tgt.AVG_LYFY_QTY_JT_BTG                    = src.AVG_LYFY_QTY_JT_BTG,
            tgt.AVG_CTCM_VS_AVG_LYFY_CHANGE            = src.AVG_CTCM_VS_AVG_LYFY_CHANGE,
            tgt.AVG_CTCM_VS_AVG_LYFY_CHANGE_PCT        = src.AVG_CTCM_VS_AVG_LYFY_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_LYFY_DIRECTION         = src.AVG_CTCM_VS_AVG_LYFY_DIRECTION,
            -- [2] INVENTORY
            tgt.CTCM_QTY_INVENTORY                              = src.CTCM_QTY_INVENTORY,
            tgt.BCTCM_QTY_INVENTORY                             = src.BCTCM_QTY_INVENTORY,
            tgt.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE             = src.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE,
            tgt.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE_PCT         = src.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE_PCT,
            tgt.CTCM_VS_BCTCM_QTY_INVENTORY_DIRECTION          = src.CTCM_VS_BCTCM_QTY_INVENTORY_DIRECTION,
            tgt.AVG_CTCM_QTY_INVENTORY                         = src.AVG_CTCM_QTY_INVENTORY,
            tgt.AVG_PTCM_QTY_INVENTORY                         = src.AVG_PTCM_QTY_INVENTORY,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE      = src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_DIRECTION   = src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_DIRECTION,
            tgt.AVG_LYFY_QTY_INVENTORY                         = src.AVG_LYFY_QTY_INVENTORY,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE      = src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_DIRECTION   = src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_DIRECTION,
            -- [3] SALES MIN
            tgt.CTCM_QTY_SALES_MIN                              = src.CTCM_QTY_SALES_MIN,
            tgt.BCTCM_QTY_SALES_MIN                             = src.BCTCM_QTY_SALES_MIN,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE             = src.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE_PCT         = src.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE_PCT,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MIN_DIRECTION          = src.CTCM_VS_BCTCM_QTY_SALES_MIN_DIRECTION,
            tgt.AVG_CTCM_QTY_SALES_MIN                         = src.AVG_CTCM_QTY_SALES_MIN,
            tgt.AVG_PTCM_QTY_SALES_MIN                         = src.AVG_PTCM_QTY_SALES_MIN,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE      = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_DIRECTION   = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_DIRECTION,
            tgt.AVG_LYFY_QTY_SALES_MIN                         = src.AVG_LYFY_QTY_SALES_MIN,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE      = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_DIRECTION   = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_DIRECTION,
            -- [4] SALES MAX
            tgt.CTCM_QTY_SALES_MAX                              = src.CTCM_QTY_SALES_MAX,
            tgt.BCTCM_QTY_SALES_MAX                             = src.BCTCM_QTY_SALES_MAX,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE             = src.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE_PCT         = src.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE_PCT,
            tgt.CTCM_VS_BCTCM_QTY_SALES_MAX_DIRECTION          = src.CTCM_VS_BCTCM_QTY_SALES_MAX_DIRECTION,
            tgt.AVG_CTCM_QTY_SALES_MAX                         = src.AVG_CTCM_QTY_SALES_MAX,
            tgt.AVG_PTCM_QTY_SALES_MAX                         = src.AVG_PTCM_QTY_SALES_MAX,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE      = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_DIRECTION   = src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_DIRECTION,
            tgt.AVG_LYFY_QTY_SALES_MAX                         = src.AVG_LYFY_QTY_SALES_MAX,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE      = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE_PCT  = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE_PCT,
            tgt.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_DIRECTION   = src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_DIRECTION

        -- ── NOT MATCHED: full insert ─────────────────────────────────────────
        WHEN NOT MATCHED THEN INSERT (
            -- Dimension keys
            CUR_TAHUN, CUR_MINGGU, RSO, AREA, DSO_SUB, PRODUCT_ID, COMPANY, COMPANY_PRINCIPAL,
            -- [1] JT BTG
            CTCM_QTY_JT_BTG,  BCTCM_QTY_JT_BTG,
            CTCM_VS_BCTCM_CHANGE,       CTCM_VS_BCTCM_CHANGE_PCT,       CTCM_VS_BCTCM_DIRECTION,
            AVG_CTCM_QTY_JT_BTG,        AVG_PTCM_QTY_JT_BTG,
            AVG_CTCM_VS_AVG_PTCM_CHANGE,       AVG_CTCM_VS_AVG_PTCM_CHANGE_PCT,       AVG_CTCM_VS_AVG_PTCM_DIRECTION,
            AVG_LYFY_QTY_JT_BTG,
            AVG_CTCM_VS_AVG_LYFY_CHANGE,       AVG_CTCM_VS_AVG_LYFY_CHANGE_PCT,       AVG_CTCM_VS_AVG_LYFY_DIRECTION,
            -- [2] INVENTORY
            CTCM_QTY_INVENTORY,         BCTCM_QTY_INVENTORY,
            CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE,        CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE_PCT,        CTCM_VS_BCTCM_QTY_INVENTORY_DIRECTION,
            AVG_CTCM_QTY_INVENTORY,     AVG_PTCM_QTY_INVENTORY,
            AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE, AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE_PCT, AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_DIRECTION,
            AVG_LYFY_QTY_INVENTORY,
            AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE, AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE_PCT, AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_DIRECTION,
            -- [3] SALES MIN
            CTCM_QTY_SALES_MIN,         BCTCM_QTY_SALES_MIN,
            CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE,        CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE_PCT,        CTCM_VS_BCTCM_QTY_SALES_MIN_DIRECTION,
            AVG_CTCM_QTY_SALES_MIN,     AVG_PTCM_QTY_SALES_MIN,
            AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE, AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE_PCT, AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_DIRECTION,
            AVG_LYFY_QTY_SALES_MIN,
            AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE, AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE_PCT, AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_DIRECTION,
            -- [4] SALES MAX
            CTCM_QTY_SALES_MAX,         BCTCM_QTY_SALES_MAX,
            CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE,        CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE_PCT,        CTCM_VS_BCTCM_QTY_SALES_MAX_DIRECTION,
            AVG_CTCM_QTY_SALES_MAX,     AVG_PTCM_QTY_SALES_MAX,
            AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE, AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE_PCT, AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_DIRECTION,
            AVG_LYFY_QTY_SALES_MAX,
            AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE, AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE_PCT, AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_DIRECTION
        ) VALUES (
            -- Dimension keys
            src.CUR_TAHUN, src.CUR_MINGGU, src.RSO, src.AREA, src.DSO_SUB, src.PRODUCT_ID, src.COMPANY, src.COMPANY_PRINCIPAL,
            -- [1] JT BTG
            src.CTCM_QTY_JT_BTG,  src.BCTCM_QTY_JT_BTG,
            src.CTCM_VS_BCTCM_CHANGE,       src.CTCM_VS_BCTCM_CHANGE_PCT,       src.CTCM_VS_BCTCM_DIRECTION,
            src.AVG_CTCM_QTY_JT_BTG,        src.AVG_PTCM_QTY_JT_BTG,
            src.AVG_CTCM_VS_AVG_PTCM_CHANGE,       src.AVG_CTCM_VS_AVG_PTCM_CHANGE_PCT,       src.AVG_CTCM_VS_AVG_PTCM_DIRECTION,
            src.AVG_LYFY_QTY_JT_BTG,
            src.AVG_CTCM_VS_AVG_LYFY_CHANGE,       src.AVG_CTCM_VS_AVG_LYFY_CHANGE_PCT,       src.AVG_CTCM_VS_AVG_LYFY_DIRECTION,
            -- [2] INVENTORY
            src.CTCM_QTY_INVENTORY,         src.BCTCM_QTY_INVENTORY,
            src.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE,        src.CTCM_VS_BCTCM_QTY_INVENTORY_CHANGE_PCT,        src.CTCM_VS_BCTCM_QTY_INVENTORY_DIRECTION,
            src.AVG_CTCM_QTY_INVENTORY,     src.AVG_PTCM_QTY_INVENTORY,
            src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE, src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_CHANGE_PCT, src.AVG_CTCM_VS_AVG_PTCM_QTY_INVENTORY_DIRECTION,
            src.AVG_LYFY_QTY_INVENTORY,
            src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE, src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_CHANGE_PCT, src.AVG_CTCM_VS_AVG_LYFY_QTY_INVENTORY_DIRECTION,
            -- [3] SALES MIN
            src.CTCM_QTY_SALES_MIN,         src.BCTCM_QTY_SALES_MIN,
            src.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE,        src.CTCM_VS_BCTCM_QTY_SALES_MIN_CHANGE_PCT,        src.CTCM_VS_BCTCM_QTY_SALES_MIN_DIRECTION,
            src.AVG_CTCM_QTY_SALES_MIN,     src.AVG_PTCM_QTY_SALES_MIN,
            src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE, src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_CHANGE_PCT, src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MIN_DIRECTION,
            src.AVG_LYFY_QTY_SALES_MIN,
            src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE, src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_CHANGE_PCT, src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MIN_DIRECTION,
            -- [4] SALES MAX
            src.CTCM_QTY_SALES_MAX,         src.BCTCM_QTY_SALES_MAX,
            src.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE,        src.CTCM_VS_BCTCM_QTY_SALES_MAX_CHANGE_PCT,        src.CTCM_VS_BCTCM_QTY_SALES_MAX_DIRECTION,
            src.AVG_CTCM_QTY_SALES_MAX,     src.AVG_PTCM_QTY_SALES_MAX,
            src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE, src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_CHANGE_PCT, src.AVG_CTCM_VS_AVG_PTCM_QTY_SALES_MAX_DIRECTION,
            src.AVG_LYFY_QTY_SALES_MAX,
            src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE, src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_CHANGE_PCT, src.AVG_CTCM_VS_AVG_LYFY_QTY_SALES_MAX_DIRECTION
        );

    END IF;

    RETURN ''Merge Data Trend Sales By DSO Sub Success.'';
END;
';