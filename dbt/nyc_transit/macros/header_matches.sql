{#
  true when an alert header mentions any of the phrases

  the mta subway alerts feed never fills in the gtfs effect enum so
  route_alert_status has to read the header to tell a suspension from a delay
  phrases match case insensitively anywhere in the text prefixes only at the
  start which is what "no [6] between ..." needs without matching every header
  that happens to contain the word no
#}
{% macro header_matches(column, phrases=[], prefixes=[]) %}
    (
        {%- for phrase in phrases %}
        lower({{ column }}) like '%{{ phrase | lower }}%'
        {%- if not loop.last or prefixes %} or {% endif %}
        {%- endfor %}
        {%- for prefix in prefixes %}
        lower({{ column }}) like '{{ prefix | lower }}%'
        {%- if not loop.last %} or {% endif %}
        {%- endfor %}
    )
{% endmacro %}
