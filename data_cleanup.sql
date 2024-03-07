WITH raw_data AS (

    SELECT
        DATE(report_date) AS date,
        campaign,
        ad_group,
        segments,
        metrics,
        customer,
        ROW_NUMBER()
            OVER (PARTITION BY DATE(report_date) AS date, campaign, ad_group, segments, metrics, customer ) AS row_number
    FROM
        `PRJOJECT_ID.DATASET_ID.TABLE_ID`

),


final AS (

    SELECT
        report_date,
        campaign,
        ad_group,
        segments,
        metrics,
        customer,
    FROM
        raw_data
    WHERE
        row_number = 1
)

SELECT * FROM final
