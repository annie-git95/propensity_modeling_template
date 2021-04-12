SELECT
  clientId,
  predicted_{{params.label_column}},
  predicted_{{params.label_column}}_probs,
    PARSE_DATE('%Y-%m-%d','{{ds}}') AS prediction_date
FROM ML.PREDICT(
    MODEL `{{params.model_id}}`,
    (SELECT *
     FROM `{{params.project_id ~ "." ~ params.propensity_dataset ~ "." ~ params.prediction_table}}`
     WHERE target_date =  PARSE_DATE('%Y-%m-%d','{{ds}}')
     ))
    , UNNEST(predicted_{{params.label_column}}_probs) AS proba