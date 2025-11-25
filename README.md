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
