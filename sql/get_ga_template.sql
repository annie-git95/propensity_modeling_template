-- interactions in last month for validation purpose
-- This section needs to be modified manually to include custom features and
-- our bool columns that are part of the conversion label
with interaction as (
	SELECT 
    clientId,
    --These are the terms used to classify a subscribed user
        max(if(h.transaction.transactionid is null,  0 , 1)) as has_converted

    -- MAX(IF(REGEXP_CONTAINS(LOWER(h.page.pagePath),r"/buy/success/") = True,1,0)) as has_converted
    -- These are interactions of value for explanatory vars 
    
    -- ENTER VARIABLES HERE
    FROM `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`, UNNEST(hits) as h
	WHERE
        _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
		AND clientId != 'null'
    
	GROUP BY clientId
),

-- hit events in last month
hit_base as (
	SELECT
		clientId,
		visitId,
		h.hitNumber as hitNumber,
		TIMESTAMP_micros(visitStartTime*1000000+h.time*1000) as hits_time,
		h.page.pagePath as pagePath,
		h.type as hits_type,
		h.eventInfo.eventCategory as hits_eventInfo_eventCategory, -- Can set these to handle NULL values so we can use a proper ARRAY_AGG function in session table
		h.eventInfo.eventAction as hits_eventInfo_eventAction,
		TIMESTAMP_micros(visitStartTime*1000000) as visit_start_time,
		channelGrouping,
		totals.newVisits as totals_newVisits,
		trafficSource.medium as trafficSource_medium,
		trafficSource.source as trafficSource_source,
		trafficSource.isTrueDirect as trafficSource_isTrueDirect,
		trafficSource.keyword as trafficSource_keyword,
		device.browser as device_browser,
		device.operatingSystem as device_operatingSystem,
		device.isMobile as device_isMobile,
		device.deviceCategory as device_deviceCategory,
		geoNetwork.region as geoNetwork_region,
		geoNetwork.latitude as geoNetwork_latitude,
		geoNetwork.longitude as geoNetwork_longitude,
		totals.sessionQualityDim as sessionQualityDim,

		TIMESTAMP_SUB(timestamp_seconds(visitId), INTERVAL 30 DAY) AS visit_id_minus_30_days
	from `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`, UNNEST(hits) as h
	WHERE
        _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
		and h.isInteraction is true
        AND clientId != 'null'
),

-- find the session and hit number before conversion
-- Simplified the category to be 'lead' or form submissions
hit_clean as (
	SELECT
		clientId,
		min(visitId) as visitId,
		ARRAY_AGG(h.hitNumber ORDER BY visitId ASC, h.hitNumber ASC LIMIT 1)[OFFSET(0)] as hitNumber
	FROM `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`, UNNEST(hits) as h
	WHERE
        _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
		AND (
             ((h.page.pagePath like "%/buy/processPayU%"))
			)
        AND clientId != 'null'
	group by clientId
),

-- remove hit events after the conversion and roll up to session level
-- Array_agg has some null values so the query breaks is not formatted as a string
session as (
	select
		orig.clientId as clientId,
		orig.visitId as visitId,
		count(orig.hitNumber) as count_hit,
		FORMAT("%T", ARRAY_AGG(orig.pagePath ORDER BY orig.hitNumber ASC)) as hits_pagePath,
		FORMAT("%T", ARRAY_AGG(orig.hits_time ORDER BY orig.hitNumber ASC)) as hits_time,
		FORMAT("%T", ARRAY_AGG(orig.hits_eventInfo_eventCategory ORDER BY orig.hitNumber ASC)) as hits_eventCategory,
		FORMAT("%T", ARRAY_AGG(orig.hits_eventInfo_eventAction ORDER BY orig.hitNumber ASC)) as hits_eventAction,
		FORMAT("%T", ARRAY_AGG(orig.hits_type ORDER BY orig.hitNumber ASC)) as hits_type,
		min(orig.trafficSource_keyword) as trafficSource_keyword,
		min(orig.channelGrouping) as channelGrouping,
		coalesce(min(orig.totals_newVisits),0) as totals_newVisits,
		min(orig.trafficSource_medium) as trafficSource_medium,
		min(orig.trafficSource_source) as trafficSource_source,
		min(orig.trafficSource_isTrueDirect) as trafficSource_isTrueDirect,
		min(orig.device_browser) as device_browser,
		min(orig.device_operatingSystem) as device_operatingSystem,
		min(orig.device_isMobile) as device_isMobile,
		min(orig.device_deviceCategory) as device_deviceCategory,
		min(orig.geoNetwork_region) as geoNetwork_region,
		min(orig.geoNetwork_latitude) as geoNetwork_latitude,
		min(orig.geoNetwork_longitude) as geoNetwork_longitude,
        timestamp_diff(max(orig.hits_time), min(orig.hits_time), second) as session_time_span,
		case when (count(distinct orig.pagePath) > 1) then timestamp_diff(max(orig.hits_time), min(orig.hits_time), second)
			else 0 end as session_time_span_page,
		max(case when (orig.hits_eventInfo_eventCategory in ("youtube")) then 1
			else 0 end) as youtube,
		case when (count(distinct orig.pagePath) < 2) then 1
			else 0 end as single_page,
		case when (timestamp_diff(max(orig.hits_time), min(orig.hits_time), second) = 0) then 1
			else 0 end as bounce,
		max(sessionQualityDim) as sessionQualityDim
	from hit_base as orig
	left join hit_clean as lead
		on orig.clientId = lead.clientId
	where
		-- exclude all events after and during conversion
		((orig.visitId < lead.visitId)
		or ((orig.visitId = lead.visitId) and (orig.hitNumber < lead.hitNumber))
		or (lead.visitId is Null))
		-- and orig.hits_eventInfo_eventCategory in ('View', 'Click')
		AND orig.clientId != 'null'
	group by clientId, visitId
),

-- Trimming any visits beyond 30 days from the dataset
session_trim AS(
  SELECT
      s.clientId,
      MAX(s.visitId) AS last_visitId,
      --Subtract 30 days from the last visit
      UNIX_SECONDS(TIMESTAMP_SUB(timestamp_seconds(MAX(s.visitId)), INTERVAL {{params.lookback_window_days}} DAY)) AS visitId_threshold
  FROM session s
  GROUP BY clientId
),

-- roll up to user level
user as (
	select
		s.clientId as clientId,
		count(distinct visitId) as count_session,
		sum(count_hit) as count_hit,
		ARRAY_AGG(visitId ORDER BY visitId ASC) AS hits_visitId,

		--Cutoff for the 30 previous mark
		MAX(st.last_visitId) AS last_visitId,
		MAX(st.visitId_threshold) AS visitId_threshold,

		ARRAY_AGG(hits_pagePath ORDER BY visitId ASC) AS hits_pagePath,
		ARRAY_AGG(hits_time ORDER BY visitId ASC) AS hits_time,
		ARRAY_AGG(hits_eventCategory ORDER BY visitId ASC) AS hits_eventCategory,
		ARRAY_AGG(hits_eventAction ORDER BY visitId ASC) AS hits_eventAction,
		ARRAY_AGG(hits_type ORDER BY visitId ASC) AS hits_type,
		ARRAY_AGG(session_time_span ORDER BY visitId ASC) AS session_time_rec,
		ARRAY_AGG(trafficSource_keyword ORDER BY visitId ASC) AS trafficSource_keyword,
		ARRAY_AGG(channelGrouping ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as channelGrouping,
		coalesce(ARRAY_AGG(totals_newVisits ORDER BY visitId DESC LIMIT 1)[OFFSET(0)],0) as totals_newVisits,
		ARRAY_AGG(trafficSource_medium ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as trafficSource_medium,
		ARRAY_AGG(trafficSource_source ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as trafficSource_source,
		IF(ARRAY_AGG(trafficSource_isTrueDirect ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] is NULL, 0, 1) AS trafficSource_isTrueDirect,
		ARRAY_AGG(device_browser ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as device_browser,
		ARRAY_AGG(device_operatingSystem ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as device_operatingSystem,
		ARRAY_AGG(device_isMobile ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as device_isMobile,
		ARRAY_AGG(device_deviceCategory ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as device_deviceCategory,
		ARRAY_AGG(geoNetwork_region ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as geoNetwork_region,
		ARRAY_AGG(geoNetwork_latitude ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as geoNetwork_latitude,
		ARRAY_AGG(geoNetwork_longitude ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as geoNetwork_longitude,
		sum(session_time_span) as historic_session,
		sum(session_time_span_page) as historic_session_page,
		ARRAY_AGG(session_time_span ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as current_session,
		sum(session_time_span)/count(distinct visitId) as avg_session_time,
		sum(session_time_span_page)/count(distinct visitId) as avg_session_time_page,
		max(youtube) as youtube,
		sum(single_page)/count(distinct visitId) as single_page_rate,
		sum(bounce)/count(distinct visitId) as bounce_rate,
		ARRAY_AGG(sessionQualityDim ORDER BY visitId DESC LIMIT 1)[OFFSET(0)] as sessionQualityDim
	from session s
	LEFT JOIN session_trim st
	ON s.clientId = st.clientId
	WHERE s.clientId != 'null'
	AND visitId > st.visitId_threshold
	group by 1
),

-- create user base from last period
daily as (
	SELECT
		distinct clientid,
	from `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`
	where
		_TABLE_SUFFIX between '{{ macros.ds_format(@target_date, "%Y-%m-%d", "%Y%m%d") }}'
        and '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
		AND clientId != 'null'
),

visits AS (
    SELECT
        clientId,
        --earliest visits
        min(visitNumber) AS earliest_visit_number,
        min(date) AS date_earliest_visit,
        min(visitId) AS earliest_visit_id,
        --latest visits
        max(visitNumber) AS latest_visit_number,
        max(date) AS date_latest_visit,
        max(visitId) AS latest_visit_id,
        --visit frequency
        DATE_DIFF(PARSE_DATE("%Y%m%d", max(date)),
            PARSE_DATE("%Y%m%d",min(date)),DAY) as days_since_first_visit,
        SAFE_DIVIDE(COUNT(visitNumber),
            DATE_DIFF(PARSE_DATE("%Y%m%d",max(date)),
                PARSE_DATE("%Y%m%d",min(date)),DAY) + 1) AS visits_per_day,
        --visit times in decimal format
        ROUND(EXTRACT( HOUR FROM TIMESTAMP_MICROS(min(visitStartTime) * CAST(1e6 AS int64))) +
            EXTRACT(MINUTE FROM TIMESTAMP_MICROS(min(visitStartTime) * CAST(1e6 AS int64))) / 60, 2) AS time_earliest_visit,
        ROUND(EXTRACT( HOUR FROM TIMESTAMP_MICROS(max(visitStartTime) * CAST(1e6 AS int64))) +
            EXTRACT(MINUTE FROM TIMESTAMP_MICROS(max(visitStartTime) * CAST(1e6 AS int64))) / 60, 2)  AS time_latest_visit,
        ROUND(AVG(ROUND(EXTRACT( HOUR FROM TIMESTAMP_MICROS(visitStartTime * CAST(1e6 AS int64))) +
        EXTRACT(MINUTE FROM TIMESTAMP_MICROS(visitStartTime * CAST(1e6 AS int64))) / 60, 2))) AS avg_visit_time,
        --bounce rate
        COUNT(totals.bounces) AS bounces, --Count ignores null values
        ROUND(SAFE_DIVIDE(COUNT(totals.bounces),
            COUNT(distinct visitId)), 2) AS bounce_rate
    FROM
    `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`
    LEFT JOIN user
        USING (clientId)
  WHERE
     _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
    -- Remove any visit data before our window
     AND visitId >= user.visitId_threshold
    -- Remove visits that are past the last visitId
     AND visitId <= user.last_visitId
  GROUP BY clientId
),

-- Counting only hits that are interactions
num_interactions AS(
    SELECT
      clientId,
      COUNT(h.isInteraction) num_interactions
    FROM
      `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`, UNNEST(hits) h
    WHERE
    _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
      AND h.isInteraction IS NOT NULL
    GROUP BY clientId
),

-- This is used to find days since the most recent last visit
last_visit AS(
    SELECT
        clientId,
        MAX(visitNumber) AS last_visit_number,
        max(visitStartTime) AS time_last_visit,
        MAX(date) AS date_last_visit,
        MAX(visitId) As last_visit_id,
         --Adding 1 to make distinction between 0 being null and 1 being same day
        DATE_DIFF(PARSE_DATE("%Y%m%d", MAX(v.date_latest_visit)),
            PARSE_DATE("%Y%m%d", MAX(date)),DAY) + 1 as days_since_last_visit,
    FROM
        `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`
    LEFT JOIN
        visits v USING (clientId)
    WHERE
        _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
    -- Get the next to last visit number
    AND visitNumber < latest_visit_number
    GROUP BY clientId

),

-- Table for source data so can link with specific clientIds and visitIds
source_t AS(
    SELECT
        clientId,
        trafficSource.source AS traffic_source,
        trafficSource.medium AS traffic_medium,
        trafficSource.keyword AS traffic_keyword,
        trafficSource.isTrueDirect As traffic_isTrueDirect,
        visitId
    FROM
        `{{params.source_project_id ~"."~ params.source_dataset ~"."~ params.source_table_prefix}}_*`
    WHERE
        _TABLE_SUFFIX BETWEEN '{{ macros.ds_format(
            macros.ds_add(@target_date, - params.lookback_window_days),
                "%Y-%m-%d", "%Y%m%d") }}'
        AND '{{ macros.ds_format(
            macros.ds_add(@target_date, + params.target_day_window_train),
                "%Y-%m-%d", "%Y%m%d") }}'
)

select
	d.clientId,
	user.count_session,
	user.count_hit,
	user.channelGrouping,
	user.totals_newVisits,
	user.device_browser,
	user.device_operatingSystem,
	user.device_isMobile,
	user.device_deviceCategory,
	user.geoNetwork_region,
	CAST(user.geoNetwork_latitude AS FLOAT64) AS geoNetwork_latitude,
	CAST(user.geoNetwork_longitude AS FLOAT64) AS geoNetwork_longitude,
	user.historic_session,
	user.historic_session_page,
	user.current_session,
	user.youtube,
	user.avg_session_time,
	user.avg_session_time_page,
	user.single_page_rate,
	user.sessionQualityDim,
	-- Removing these arrays for now
	--user.hits_visitId,
	--user.hits_pagePath,
	--user.hits_time,
	--user.hits_eventCategory,
	--user.hits_eventAction,
	--user.hits_type,

	--Removing this record of session times
	--user.session_time_rec,

	--Removing these traficSource tracking arrays
	--My new query just looks at first and latest keywords
	--user.trafficSource_medium,
	--user.trafficSource_source,
	--user.trafficSource_keyword,
	--user.trafficSource_isTrueDirect,

	user.last_visitId,
    v.latest_visit_id,
	user.visitId_threshold,
    v.earliest_visit_id,
	--Visit data
	v.earliest_visit_number,
    v.latest_visit_number,
    v.time_earliest_visit,
    v.time_latest_visit,
	v.avg_visit_time,
	IFNULL(lav.days_since_last_visit, 0) AS days_since_last_visit,
    v.days_since_first_visit,
    v.visits_per_day,
    v.bounce_rate,
    --Source Data
    es.traffic_source AS earliest_source,
    ls.traffic_source AS latest_source,
    es.traffic_medium AS earliest_medium,
    ls.traffic_medium AS latest_medium,
    es.traffic_keyword AS earliest_keyword,
    ls.traffic_keyword AS latest_keyword,
    IFNULL(es.traffic_isTrueDirect, false) AS earliest_isTrueDirect,
    IFNULL(ls.traffic_isTrueDirect, false) AS latest_isTrueDirect,
    ni.num_interactions,
    --This is where you put your custom variables
    -- ENTER VARS HERE
   -- s.scroll_track,
   -- s.ntg_article_milestore,
  --  s.video,
    --This is the target variable
    s.has_converted,
    PARSE_DATE('%Y-%m-%d', '{{@target_date}}') AS target_date
from daily d
left join interaction s
	on d.clientId = s.clientId
left join user
	on d.clientId = user.clientId
LEFT JOIN visits v
  ON d.clientId = v.clientId
LEFT JOIN last_visit lav
    ON d.clientId = lav.clientId
LEFT JOIN source_t AS es --earliest_source
  ON d.clientId = es.clientId
  AND earliest_visit_id = es.visitId
LEFT JOIN source_t AS ls --latest_source
    ON d.clientId = ls.clientId
    AND latest_visit_id = ls.visitId
LEFT JOIN num_interactions ni
    ON d.clientId = ni.clientId
WHERE
	-- remove users converted before a month
	user.count_session is not null
