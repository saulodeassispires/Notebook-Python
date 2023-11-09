{{ config(materialized='table') }}

WITH OPPORTUNITY AS (
    SELECT
        ID::VARCHAR AS opportunity_id,
        name,
        STAGENAME AS stage_name,
        amount::DOUBLE AS amount,
        NEXTSTEP as next_step,
        CLOSEDATE::TIMESTAMP_NTZ as close_date,
        FORECASTED__C AS forecasted,
        -- using custom_probability field instead of build in salesforece
        CUSTOM_PROBABILITY__C::double as probability,
        -- SYSYSTEM_DATE for unified dates
        CLOSEDATE::TIMESTAMP_NTZ as DATE
    FROM
        PRD_KEBOOLA_SOURCES.SALESFORCE.OPPORTUNITY
        
),
CUSTOMER AS (
    -- getting salesformce
    SELECT
        ID::VARCHAR AS customer_id,
        name
    FROM
        PRD_KEBOOLA_SOURCES.SALESFORCE.ACCOUNT
    UNION ALL
    -- Getting SAP
    SELECT
        CARDCODE::VARCHAR AS customer_id,
        CARDNAME AS name
    FROM
    PRD_ETLEAP_SOURCES.SAP.OCRD 
    -- Adding a filter for the card type from OCRD of 'C' for Customer
    WHERE CARDTYPE = 'C'
    
),
SALES_ORDER AS
(
 SELECT
     DOCNUM::VARCHAR AS sales_order_id,
     DOCTOTAL::DOUBLE AS amount,
     DOCDATE::TIMESTAMP_NTZ AS booking_date,
     -- SYSTEM_DATE for unified time.
     DOCDATE::TIMESTAMP_NTZ AS date
 FROM
     PRD_ETLEAP_SOURCES.SAP.ORDR
 GROUP BY ALL
     
),
TERRITORY AS (
    SELECT
        TERRITRYID::VARCHAR AS territory_id,
        DESCRIPT AS name
    FROM
        PRD_ETLEAP_SOURCES.SAP.OTER 
    GROUP BY ALL
    -- TODO: Need a union from SALESFORCE as there is a similar concept, territory is in and needs to be synced
    UNION ALL
      SELECT
      "Territory__c"::VARCHAR AS territory_id,
       -- in the source, there is no identifier for territory
      "Territory__c" AS name
    FROM
    PRD_KEBOOLA_SOURCES.SALESFORCE.OPPORTUNITY 
      GROUP BY ALL
    
),
EMPLOYEE AS (
    SELECT
        ID::VARCHAR AS employee_id,
        NAME AS Full_Name
    FROM
    PRD_KEBOOLA_SOURCES.SALESFORCE."USER"
    GROUP BY ALL
    -- NOTE: There is currently not a way to tell between uses and employees in salesforce
),
SALES_TARGET AS (
    SELECT
        sales_target_id,
        date,
        amount
    FROM 
        PRD_DW.PRODUCT.REFERENCE_DAILY_SALES_TARGETS
),

-- making link tables for relationships
link_opportunity AS (
    -- generating a range of dates
  --  SELECT core.*, dateadd(day, v.index, to_date(core.effective_from_date))::TIMESTAMP_NTZ as date
  --  FROM (
        -- core section
        SELECT 
              ACCOUNTID::VARCHAR AS customer_id,
              ID::VARCHAR AS opportunity_id,
              OWNERID::VARCHAR AS employee_id,
              -- getting the earliest business date from the source table
              min(CREATEDDATE::TIMESTAMP_NTZ) AS effective_from_date,
              -- the relationship is active until today
              CURRENT_DATE::TIMESTAMP_NTZ AS effective_end_date
          FROM
          PRD_KEBOOLA_SOURCES.SALESFORCE.OPPORTUNITY 
          GROUP BY ALL
   -- ) as core, 
   --    lateral flatten (split(space(datediff(day,to_date(core.effective_from_date), nvl(to_date(core.effective_end_date), current_date))),' ')) v
),
link_opportunity_sales_order AS (
    -- generating a range of dates
  --  SELECT core.*, dateadd(day, v.index, to_date(core.effective_from_date))::TIMESTAMP_NTZ as date
  --  FROM (
        -- core section
        SELECT 
              ID::VARCHAR AS opportunity_id,
              SALES_ORDER__C::VARCHAR AS sales_order_id,
              -- getting the earliest business date from the source table
              min(CREATEDDATE::TIMESTAMP_NTZ) AS effective_from_date,
              -- the relationship is active until today
              CURRENT_DATE::TIMESTAMP_NTZ AS effective_end_date
          FROM
          PRD_KEBOOLA_SOURCES.SALESFORCE.OPPORTUNITY 
          GROUP BY ALL
 --   ) as core, 
   --    lateral flatten (split(space(datediff(day,to_date(core.effective_from_date), nvl(to_date(core.effective_end_date), current_date))),' ')) v
),
-- link to customer to sales_order,
link_sales_order AS (
--    SELECT core.*, dateadd(day, v.index, to_date(core.effective_from_date))::TIMESTAMP_NTZ as date
 --   FROM (
        -- core section
      SELECT 
        CARDCODE::VARCHAR AS customer_id,
        DOCNUM::VARCHAR AS sales_order_id,
        min(CREATEDATE::TIMESTAMP_NTZ) AS effective_from_date,
        -- the relationship is active until today
        CURRENT_DATE::TIMESTAMP_NTZ AS effective_end_date
      FROM 
      PRD_ETLEAP_SOURCES.SAP.ORDR
      GROUP BY ALL
  --  ) as core, 
  --     lateral flatten (split(space(datediff(day,to_date(core.effective_from_date), nvl(to_date(core.effective_end_date), current_date))),' ')) v
),
-- linking customers and territories
link_customer_territory AS (
  --  SELECT core.*, dateadd(day, v.index, to_date(core.effective_from_date))::TIMESTAMP_NTZ as date
  --  FROM (
        -- core section
      select 
          CARDCODE::VARCHAR AS customer_id,
          TERRITORY::VARCHAR AS territory_id,
          min(CREATEDATE::TIMESTAMP_NTZ) AS effective_from_date,
          -- the relationship is active until today
          CURRENT_DATE::TIMESTAMP_NTZ AS effective_end_date
      FROM
      PRD_ETLEAP_SOURCES.SAP.OCRD
      GROUP BY ALL

      UNION ALL

      SELECT
          ACCOUNTID::VARCHAR AS customer_id,
          "Territory__c"::VARCHAR AS territory_id,
              -- getting the earliest business date from the source table
          min(CREATEDDATE::TIMESTAMP_NTZ) AS effective_from_date,
          -- the relationship is active until today
          CURRENT_DATE::TIMESTAMP_NTZ AS effective_end_date
          FROM
          PRD_KEBOOLA_SOURCES.SALESFORCE.OPPORTUNITY 
          GROUP BY ALL
      
--    ) as core, 
   --    lateral flatten (split(space(datediff(day,to_date(core.effective_from_date), nvl(to_date(core.effective_end_date), current_date))),' ')) v
),
-- building the bridge between links for the business
bridge_opportunity AS (
    SELECT
        link_opportunity.customer_id,
        link_opportunity.opportunity_id,
        link_customer_territory.territory_id,
        link_opportunity.employee_id::VARCHAR as employee_id,
        opportunity.date AS date
    FROM
        link_opportunity
        LEFT JOIN link_customer_territory
        ON link_opportunity.customer_id = link_customer_territory.customer_id
        -- joining with opportunity to get the list of dates for the bridge
        LEFT JOIN OPPORTUNITY
        ON link_opportunity.opportunity_id = opportunity.opportunity_id
        
        
),
-- building the bridge between links for the business needs
bridge_sales_order AS (
    SELECT
        link_sales_order.customer_id,
        link_sales_order.sales_order_id,
        link_opportunity_sales_order.opportunity_id,
        link_customer_territory.territory_id,
        -- TODO: Add employee_Id when if becomes needed from SAP
        sales_order.date as date
    FROM
        link_sales_order
        LEFT JOIN link_customer_territory
        ON link_sales_order.customer_id = link_customer_territory.customer_id
        LEFT JOIN link_opportunity_sales_order
        ON link_opportunity_sales_order.sales_order_id = link_sales_order.sales_order_id
        -- joining sales_order to get the date
        LEFT JOIN SALES_ORDER
        ON link_sales_order.sales_order_id = sales_order.sales_order_id
),
-- bridge of bridges union for full outer join effect. 
-- This approach is based on the "puppini brige" from the Unified Star Schema
gateway AS (
    SELECT
        'bridge_sales_order' AS source,
        customer_id,
        opportunity_id,
        sales_order_id,
        NULL as employee_id,
        territory_id,
        NULL::VARCHAR AS sales_target_id,
        date
    FROM
        bridge_sales_order
        
    UNION ALL

    SELECT
        'bridge_opportunity' AS source,
        customer_id,
        opportunity_id,
        NULL::VARCHAR AS sales_order_id,
        employee_id,
        territory_id,
        NULL::VARCHAR AS sales_target_id,
        date
    FROM
        bridge_opportunity
    
    UNION ALL
    -- Adding in the target data
    SELECT
        'sales_target' AS source,
         NULL::VARCHAR AS customer_id,
         NULL::VARCHAR AS opportunity_id,
         NULL::VARCHAR AS sales_order_id,
         NULL::VARCHAR AS employee_id,
         NULL::VARCHAR AS territory_id,
         sales_target_id,
         date
    FROM 
        SALES_TARGET 
)
-- Gives us a curated dataset
select 
gateway.*,
territory.name as territory_name,
opportunity.name as opportunity_name,
opportunity.stage_name,
opportunity.amount as opportunity_amount,
opportunity.forecasted as opportunity_forecasted,
opportunity.probability as opportunity_probability,
opportunity.next_step,
sales_order.amount as sales_order_amount,
customer.name as customer_name,
employee.full_name as employee_full_name,
sales_target.amount as sales_target_amount
from gateway
LEFT JOIN opportunity on gateway.opportunity_id = opportunity.opportunity_id
LEFT JOIN customer on gateway.customer_id = customer.customer_id
LEFT JOIN sales_order on gateway.sales_order_id = sales_order.sales_order_id
LEFT JOIN territory on gateway.territory_id = territory.territory_id
LEFT JOIN employee on gateway.employee_id = employee.employee_id
LEFT JOIN sales_target ON gateway.sales_target_id = sales_target.sales_target_id