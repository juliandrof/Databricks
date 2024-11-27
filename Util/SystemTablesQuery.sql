-- Databricks notebook source
--Quais usuários  acessam essa tabela com maior frequência?
SELECT user_identity.email, count(*) as qnt_acessos
FROM system.access.audit
WHERE request_params.table_full_name = "jsf_catalog.ecommerce.vendas_silver"
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

--Quem apagou esta tabela?
SELECT user_identity.email, count(*) as qnt_acessos
FROM system.access.audit
WHERE request_params.table_full_name = "jsf_catalog.ecommerce.vendas_silver"
AND service_name = "unityCatalog"
AND action_name = "deleteTable"
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

--O que esse usuário acessou nas últimas 24 horas?
SELECT request_params.table_full_name
FROM system.access.audit
WHERE user_identity.email = "juliandro.figueiro@databricks.com"
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
AND datediff(now(), event_date) < 1;

--Quais tabelas esse usuário acessa com mais frequência?
SELECT request_params.table_full_name, COUNT(*) as num_acessos
FROM system.access.audit
WHERE user_identity.email = "juliandro.figueiro@databricks.com"
and request_params.table_full_name is not null
AND service_name = "unityCatalog"
AND action_name = "generateTemporaryTableCredential"
GROUP BY 1 ORDER BY 2 DESC;


--Quantas DBUs de cada SKU foram usadas até agora este mês?
SELECT
   CASE
       WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
       WHEN sku_name LIKE '%JOBS%' THEN 'JOBS'
       WHEN sku_name LIKE '%DLT%' THEN 'DLT'
       WHEN sku_name LIKE '%SQL%' THEN 'SQL'
       WHEN sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
       ELSE 'OTHER'
   END AS sku,
   sum(usage_quantity) as `DBUs`
  FROM system.billing.usage
WHERE
  month(usage_date) = month(CURRENT_DATE)
GROUP BY sku
ORDER BY `DBUs` DESC;

--Qual é a tendência diária no consumo de DBU?
SELECT date(usage_date) as `Date`, sum(usage_quantity) as `DBUs Consumed`
  FROM system.billing.usage
GROUP BY date(usage_date)
ORDER BY date(usage_date) ASC;

--Quais Jobs consumiram mais DBUs?
SELECT usage_metadata.job_id as `Job ID`, sum(usage_quantity) as `DBUs`
  FROM system.billing.usage
WHERE usage_metadata.job_id is not null
GROUP BY `Job ID`
ORDER BY `DBUs` DESC;


--Que tabelas são originadas desta tabela?
SELECT DISTINCT target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = "jsf_catalog.ecommerce.vendas_silver";


--Quais consultas de usuário leem desta tabela?
SELECT DISTINCT entity_type, entity_id, source_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = "jsf_catalog.ecommerce.vendas_silver";


--Qual o custo do Lakehouse Monitoring?
SELECT month(usage_date) mes, sum(usage_quantity) as dbus
FROM system.billing.usage
WHERE
  usage_date >= DATE_SUB(current_date(), 30) AND
  sku_name like "%JOBS_SERVERLESS%" AND
  custom_tags["LakehouseMonitoring"] = "true"
GROUP BY month(usage_date)
ORDER BY mes DESC
