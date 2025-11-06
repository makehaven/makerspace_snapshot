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
2.  **Configure SQL queries:** Navigate to `/admin/config/makerspace/snapshot` and review the dataset information on the Snapshot Data Sources tab. Update the SQL queries in the configuration YAML to match your database schema as needed. The module provides default queries that use benign example views, so the UI will render even before a real SQL is provided.
3.  **Configure snapshot interval:**  Select the desired snapshot interval from the dropdown menu on the configuration page.

## How to Set Up SQL Views

The module relies on a set of SQL views to retrieve the data for snapshots. You will need to create these views in your database. The following is a list of the required views and the expected fields for each:

-   **`your_view_active_members`**:
    -   `member_id`: The unique identifier for the member.
    -   `plan_code`: The machine name of the member's plan.
    -   `plan_label`: The human-readable label for the member's plan.
-   **`your_view_paused_members`**:
    -   `member_id`
    -   `plan_code`
    -   `plan_label`
-   **`your_view_lapsed_members`**:
    -   `member_id`
    -   `plan_code`
    -   `plan_label`
-   **`your_view_joins_in_period`**:
    -   `member_id`
    -   `plan_code`
    -   `plan_label`
    -   `joined_on`: The date the member joined, in `YYYY-MM-DD` format.
-   **`your_view_cancels_in_period`**:
    -   `member_id`
    -   `plan_code`
    -   `plan_label`
    -   `canceled_on`: The date the member canceled, in `YYYY-MM-DD` format.

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
