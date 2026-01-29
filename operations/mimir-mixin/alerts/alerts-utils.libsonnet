{
  // The alert name is prefixed with the product name (eg. AlertName -> MimirAlertName).
  alertName(name)::
    $._config.alert_product + name,

  jobMatcher(job)::
    '%s=~"%s%s"' % [$._config.per_job_label, $._config.alert_job_prefix, formatJobForQuery(job)],

  jobNotMatcher(job)::
    '%s!~"%s%s"' % [$._config.per_job_label, $._config.alert_job_prefix, formatJobForQuery(job)],

  local formatJobForQuery(job) =
    if std.isArray(job) then '(%s)' % std.join('|', job)
    else if std.isString(job) then job
    else error 'expected job "%s" to be a string or an array, but it is type "%s"' % [job, std.type(job)],

  local formatDashboardURL(filename, name) =
    if $._config.externalGrafanaURLPrefix == null then {} else {
      local url = '%(prefix)s/d/%(uid)s/%(name)s' % {
        prefix: $._config.externalGrafanaURLPrefix,
        // TODO: consider making some dashboardID's that we can index into from alerts, not just dashboards
        // uid: $._config.grafanaDashboardIDs['mimir-dashboard_name.json'],
        uid: std.md5(filename),
        name: std.asciiLower(name),
      },
      // TODO: join params with &
      // add cluster and namespace specifically?
      // take a map?
      // local queryParams = '?${datasource:queryparam}&var-cluster=$cluster&var-namespace=${__data.fields.Namespace}';
      dashboard_url: url,
    },

  dashboardURL(filename, name, cluster=null, namespace=null, params=[])::
    formatDashboardURL(filename, name),

  withDashboardURL(filename, name, groups)::
    local update_rule(rule) =
      if std.objectHas(rule, 'alert')
      then rule {
        annotations+:
          formatDashboardURL(filename, name),
      }
      else rule;
    [
      group {
        rules: [
          update_rule(alert)
          for alert in group.rules
        ],
      }
      for group in groups
    ],

  withRunbookURL(url_format, groups)::
    local update_rule(rule) =
      if std.objectHas(rule, 'alert')
      then rule {
        annotations+: {
          runbook_url: url_format % std.asciiLower(rule.alert),
        },
      }
      else rule;
    [
      group {
        rules: [
          update_rule(alert)
          for alert in group.rules
        ],
      }
      for group in groups
    ],

  withExtraLabelsAnnotations(groups)::
    local update_rule(rule) =
      if std.objectHas(rule, 'alert')
      then rule {
        annotations+: $._config.alert_extra_annotations,
        labels+: $._config.alert_extra_labels,
      }
      else rule;
    [
      group {
        rules: [
          update_rule(rule)
          for rule in group.rules
        ],
      }
      for group in groups
    ],

  alertRangeInterval(multiple)::
    ($._config.base_alerts_range_interval_minutes * multiple) + 'm',

  histogramLabels(labels, histogram_type, nhcb=false)::
    assert histogram_type == 'native' || histogram_type == 'classic';
    labels { histogram: histogram_type } +
    (if histogram_type == 'native' && nhcb then {
       buckets: 'custom',
     } else {}),
}
