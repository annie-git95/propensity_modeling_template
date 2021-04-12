SELECT
    clientId,
    CASE
    -- Loop through thresholds and labels to get all cases for binning
    {%- for threshold in params.thresholds %}
        {%- if loop.first %}
            WHEN ROUND(MAX(p.prob), 3) >= 0 AND ROUND(MAX(p.prob), 3) < {{ params.thresholds[loop.index0] }}
                THEN "{{ params.bin_labels[loop.index0] }}"
        {%- else %}
            WHEN ROUND(MAX(p.prob), 3) >= {{ params.thresholds[loop.index0 - 1] }} AND ROUND(MAX(p.prob), 3) < {{ threshold }}
                THEN "{{ params.bin_labels[loop.index0] }}"
        {%- endif %}
    {%- endfor %}
        -- Default is the highest propensity bin which is last in the label list
        ELSE "{{params.bin_labels|last}}"
    END AS predicted_{{ params.label_column }}
FROM `{{params.project_id ~ "." ~ params.propensity_dataset ~ "." ~ params.results_table}}` AS results
CROSS JOIN UNNEST(predicted_{{ params.label_column }}_probs) p
WHERE prediction_date = PARSE_DATE('%Y-%m-%d','{{ds}}')
-- Only interested in the probability of being class 1 (converter)
AND p.label = 1
GROUP BY clientId