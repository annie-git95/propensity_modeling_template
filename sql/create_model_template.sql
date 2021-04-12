create or replace MODEL
  `{{params.model_id}}`
  TRANSFORM(
    ML.STANDARD_SCALER(count_session) OVER() as count_session,
    ML.STANDARD_SCALER(count_hit) OVER() AS count_hit,
    ML.STANDARD_SCALER(totals_newVisits) OVER() AS totals_newVisits,
    ML.STANDARD_SCALER(time_on_site) OVER() AS time_on_site,
    ML.STANDARD_SCALER(bounce_rate) OVER() AS bounce_rate,
    ML.STANDARD_SCALER(num_interactions) OVER() AS num_interactions,
  --  ML.MIN_MAX_SCALER(geoNetwork_latitude) OVER() AS geoNetwork_latitude,
   -- ML.MIN_MAX_SCALER(geoNetwork_longitude) OVER() AS geoNetwork_longitude,
    ML.STANDARD_SCALER(historic_session) OVER() AS historic_session,
    ML.STANDARD_SCALER(historic_session_page) OVER() AS historic_session_page,
 --   ML.STANDARD_SCALER(current_session) OVER() AS current_session,
  --  youtube,
    ML.STANDARD_SCALER(avg_session_time) OVER() AS avg_session_time,
    ML.STANDARD_SCALER(avg_session_time_page) OVER() AS avg_session_time_page,
  --  ML.STANDARD_SCALER(single_page_rate) OVER() AS single_page_rate,

 --   ML.MIN_MAX_SCALER(days_since_first_visit) OVER() AS days_since_first_visit,
 --   ML.QUANTILE_BUCKETIZE(days_since_last_visit, 4) OVER() AS days_since_last_visit,
    ML.STANDARD_SCALER(visits_per_day) OVER() AS visits_per_day,
    earliest_source,
    earliest_medium,
    channelGrouping,
    device_operatingSystem,
    latest_source,     
   -- device_browser,
  --  device_operatingSystem,
  --  device_isMobile,
   -- device_deviceCategory,
    earliest_visit_number, 
    latest_visit_number,  
    fresh_food, 
    breakfast, 
--    cleaning_&_household, 
    pet_supplies,        
  --  geoNetwork_region,
      
    ML.MIN_MAX_SCALER(sessionQualityDim) OVER() AS sessionQualityDim,
    ML.MIN_MAX_SCALER(time_earliest_visit) OVER() AS time_earliest_visit,
    ML.MIN_MAX_SCALER(time_latest_visit) OVER() AS time_latest_visit,
    ML.MIN_MAX_SCALER(avg_visit_time) OVER() AS avg_visit_time,
    --Right now will only work for non standardized features, binary will be best for
    --custom variables. If scaling is needed, then ensure that the variable is a string
    --with the proper BQ scaler function
    {% for custom_var in params.custom_event_vars %}
        {{custom_var}},
    {% endfor %}
    --Label Column
    {{params.label_column}},
  )
  OPTIONS (model_type='logistic_reg',
   AUTO_CLASS_WEIGHTS = TRUE,
   -- CLASS_WEIGHTS = [STRUCT({{params.label_column}}, .8)],
    input_label_cols=[
        "{{params.label_column}}"
    ],
    WARM_START={{params.warm_start_bool}},
    max_iterations=50) AS (
  select
    count_session,
    count_hit,
    totals_newVisits,
    earliest_visit_number, 
    latest_visit_number,  
    fresh_food, 
    breakfast, 
    pet_supplies, 
    historic_session,
    historic_session_page,
        
   -- device_browser,
   -- device_operatingSystem,
  --  device_isMobile,
  --  device_deviceCategory,
  --  geoNetwork_region,
    avg_session_time,
    avg_session_time_page,
    sessionQualityDim,
    time_earliest_visit,
    time_latest_visit,
    avg_visit_time,
 --   days_since_first_visit,
 --   days_since_last_visit,
  --  earliest_source,
  --  latest_source,
    bounce_rate,
    num_interactions,
    visits_per_day,
        
    earliest_source,
    earliest_medium,
    channelGrouping,
    device_operatingSystem,
    latest_source, 
        
    --Custom Variables
    bounces,
    time_on_site,
    {% for custom_var in params.custom_event_vars %}
        {{custom_var}},
    {% endfor %}
    --Label Column
    {{params.label_column}}
  FROM
    `{{params.project_id}}.{{params.dataset}}.{{params.train_table}}`
  WHERE
    target_date = PARSE_DATE('%Y-%m-%d','{{prev_ds}}')
)