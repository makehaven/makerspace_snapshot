# Makerspace Snapshot Module

The Makerspace Snapshot module is a Drupal module designed to capture and store periodic snapshots of membership data for reporting and analysis. It provides a flexible and configurable way to track key metrics over time, with a focus on providing data to dashboard environments.

## Features

- **Configurable Snapshot Intervals:**  Schedule snapshots to be taken daily, monthly, quarterly, or annually.
- **Manual Snapshots:** Manually trigger snapshots for testing or ad-hoc analysis.
- **Test Data:** Mark snapshots as "test" data, allowing them to be easily identified and deleted.
- **Configurable SQL Queries:** Define the SQL queries used to generate snapshot data, allowing the module to be adapted to any database schema.
- **API Endpoints:** Exposes a set of API endpoints for retrieving snapshot data in a format suitable for dashboard consumption.
- **Data Seeding:**  Includes a data seeding mechanism to provide sample data on a fresh installation.
- **KPI Storage:** Persists any KPI values returned by `hook_makerspace_snapshot_collect_kpi()` into the `ms_fact_kpi_snapshot` table so dashboards can source metrics directly from snapshots.

## Installation

1.  **Enable the module:** Install the module as you would any other Drupal module.
2.  **Configure snapshot interval:**  Select the desired snapshot interval from the dropdown menu on the configuration page.
3.  **Review SQL sources:** Audit the default SQL in `src/SnapshotService.php::$sourceQueries`. Update the queries to match your site’s schema (typically via a patch or service override) before running the first snapshot.

## Customizing Snapshot SQL

For security reasons the module no longer stores SQL in configuration entities. All snapshot datasets use the canonical queries defined in `src/SnapshotService.php` inside the `$sourceQueries` array. Each entry contains a machine name (for example `sql_active`), a description, and a `SQL` heredoc string. If your membership data lives in different tables or views, edit the relevant `SQL` strings and redeploy the module.

Recommended workflow:

1. Copy `src/SnapshotService.php` into your project repository and apply site-specific changes under version control.
2. Keep the machine names the same so existing configuration (dataset enablement, UI labels, etc.) continues to work.
3. When possible, encapsulate custom business logic in database views or Drupal fields, then reference those artifacts in the service query to minimize future diffs.

## Development

Contributions and improvements to this module are welcome. Please refer to the `AGENTS.md` file for technical guidance on the module's architecture and development patterns.

### Adding KPI Metrics

Implement `hook_makerspace_snapshot_collect_kpi()` in your module to push additional KPI values into the snapshot pipeline. The hook receives the snapshot context (dates, membership counts, etc.) and should return an array keyed by KPI machine name. Each entry can be a scalar value or an array with optional `period_year`, `period_month`, and `meta` overrides. Captured metrics are stored in `ms_fact_kpi_snapshot` and are immediately consumable by the makerspace dashboard services.

### Adding New Snapshot Datasets

The module follows a consistent pattern for listing, importing, and exporting dataset definitions. When you introduce a new metric, touch the following pieces so every UI stays in sync:

1. **Define metadata and headers**  
   - Extend `$datasetDefinitions` and, when needed, `$datasetSourceMap` inside `src/SnapshotService.php`.  
   - Include a human-readable label, default schedules, CSV headers, acquisition mode, and data source description.

2. **Expose historical downloads (optional)**  
   - Add the dataset’s route name to `SnapshotAdminBaseForm::getHistoricalDownloadRouteMap()` if it should provide org/plan-style CSV history.  
   - Implement a controller method or alter hook to stream the historical CSV if the generic routes do not already cover it.

3. **Wire import handling**  
   - Update `SnapshotImportForm::processDatasetRows()` with parsing/validation logic for the new definition.  
   - Store the data through a dedicated helper in `SnapshotService` (similar to `importPlanLevelsSnapshot()`) so API/exports share the same schema.

4. **Templates and exports**  
   - Ensure `SnapshotTemplateController` returns a CSV template that matches the new headers.  
   - If the dataset participates in the snapshot export ZIP, extend `SnapshotService::getSnapshotExportData()` accordingly.

Every dataset keyed in `buildDefinitions()` automatically gains admin listings, import upload widgets, historical download links on the data sources tab, and entries on the Download Snapshot Data page. Keeping machine names in `snake_case`, headers aligned with `buildDefinitions()`, and CSV files in UTF-8 ensures imports remain stable across environments.


## Snapshot integrity and historical recovery

Captures commit their headers and facts in one database transaction. A failed
replacement preserves the previous run; a failed new run leaves no partial
headers. Plan codes are trimmed and case-normalized before grouping. Update
`makerspace_snapshot_update_10020` adds `completed_at`; legacy rows retain zero
because their completion cannot be inferred from a header.

`drush makerspace-snapshot:health --period=YYYY-MM-01` checks automatic-source
facts, missing KPI detail, prior-month KPI coverage and membership/plan
reconciliation. Empty variable-length datasets require a completed capture.
Automatic cron can retry an incomplete capture created today, but does not
replace older partial history with today's mutable roster. Import-only
survey definitions are excluded from automatic captures. Existing health
notifications now receive these fact-level findings.

For an existing partial monthly run, preview bounded recovery:

```sh
drush makerspace-snapshot:recover-facts --period=2026-09-01
# After reviewing the preview in the target environment:
drush makerspace-snapshot:recover-facts --period=2026-09-01 --apply
drush makerspace-snapshot:health --period=2026-09-01
```

Recovery adds only missing active-member/new-signup KPIs from the original org
fact, and missing donation facts from dated contributions in the preceding
completed month. It does not replace existing facts and is idempotent. Dated
contributions reflect the ledger as it exists at recovery time, including later
corrections. Other missing KPIs, plans, revenue, storage, certifications and
access grants remain unresolved unless historical evidence is supplied. Health
may therefore still fail after safe recovery; do not rerun a whole historical
snapshot just to clear warnings.

Storage resolves the assignment-status table through Drupal's field mapping,
uses the current billing method with the legacy complimentary fallback, and
excludes inactive units from rentable capacity. Stored monthly price fields are
assignment prices, not proof of provider billing or collected revenue.

## Early warning (nightly readings + Slack tripwires)

`src/EarlyWarning/EarlyWarningMonitor.php`, added 2026-10-27. It runs from
`hook_cron` once a day after 6am (site timezone) and does three things:

1. **Records readings.** About 20 flow metrics (event registrations, join-form
   submissions and the share with no website account, new accounts, new
   members, Chargebee sync activity, door events, CiviCRM reminders, mail
   give-ups, quizzes, store, lending and more) are stored as one row per metric
   per day in `ms_early_warning_reading`, kept for 400 days. The weekly
   workflow-health sweep in the site repo can then compare this week with last
   week instead of relying on memory.
2. **Trips tripwires.** `EarlyWarningMonitor::TRIPWIRES` is a short list of
   things that should never happen in a normal week. Thresholds come from 26
   weeks of live history (2026-04 → 2026-10). A tripwire posts to Slack
   #infrastructure when it trips (mentioning @staff), again every 3 days while
   it stays tripped and once when it clears, both without a mention. Quiet
   nights post nothing. Posts use `slack_member_sync`'s bot token, not the
   `slack_connector` webhook, because a modern webhook is locked to one channel.
   Only live posts (`PANTHEON_ENVIRONMENT`); test has the token too, so the
   token cannot be the gate.
   Channel and group can be overridden in `makerspace_snapshot.settings`
   (`early_warning_channel`, `early_warning_mention_group`; '' = no mention).
3. **Raises one-time notices.** It flags a billing plan nobody has seen before
   (the first run records the existing plans silently), and a member who joined
   in the last 14 days whose email bounced in CiviCRM or looks like a typo for
   a big provider (via `makerspace_email_guard`, when installed). Each item is
   reported once.

Every check asks "did the thing happen?" rather than "was there an error?".
The costly failures here, such as reminders silently dead from April to July
2026, wrote no error at all. A probe that throws reads as "could not read",
never as 0, and never trips a wire on its own.

```bash
drush makerspace-snapshot:early-warning           # read-only: today vs 1 and 7 days ago
drush makerspace-snapshot:early-warning --record  # the nightly run, now (stores + posts)
```

To add a metric, add a probe to `probes()`. Add a tripwire only with a
threshold read off live history, and update the unit test's `NORMAL` readings
so a normal week still trips nothing.
