/*
    Architectural test: no gold model may depend on a bronze source.

    ADR-0003 makes bronze duplicate-tolerant, so a gold model reading it
    directly would double-count without any test failing -- the numbers would
    simply be wrong. Conventions decay under deadline pressure; this one is
    enforced against the manifest at every dbt run.

    Returns one row per violating model, which fails the test.
*/

{% set violations = [] %}

{% if execute %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and 'gold' in node.fqn %}
            {% for parent in node.depends_on.nodes %}
                {% if parent.startswith('source.') and '.bronze.' in parent %}
                    {% do violations.append(node.name ~ ' -> ' ~ parent) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if violations %}
select * from (values
    {% for violation in violations %}
    ('{{ violation }}'){% if not loop.last %},{% endif %}
    {% endfor %}
) as t (violation)
{% else %}
select 'none' as violation where false
{% endif %}
