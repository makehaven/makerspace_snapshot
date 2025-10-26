# AGENTS.md

This document provides guidance for AI agents working with the Makerspace Snapshot module.

## Module Architecture

The Makerspace Snapshot module is designed to capture and store periodic snapshots of membership data for reporting and analysis. The module is built for Drupal and follows standard Drupal conventions.

### Key Components

- **Configuration:** The module's behavior is primarily driven by configuration entities, which are defined in YAML files in the `config/install` directory. These include settings, SQL query sources, and metric definitions.
- **Database:** The module uses a set of custom tables to store snapshot data. The schema is defined in `makerspace_snapshot.install`.
- **Drush Command:** A Drush command, `makerspace-snapshot:snapshot`, is provided for manually triggering snapshots. The command is defined in `src/Commands/MakerspaceSnapshotCommands.php`.
- **Admin Form:** A configuration form at `/admin/config/makerspace/snapshot` allows administrators to configure the module and manually trigger snapshots. The form is defined in `src/Form/SnapshotAdminForm.php`.
- **API:** The module exposes a set of API endpoints for retrieving snapshot data in a format suitable for dashboard consumption. The API is defined in `src/Controller/SnapshotApiController.php`.

### Development Patterns

- **Configuration Management:** All configuration is managed through Drupal's configuration management system. Any changes to the module's configuration should be made in the corresponding YAML files in the `config/install` directory.
- **Database Schema:** The database schema is managed through `hook_schema()` and `hook_update_N()` in `makerspace_snapshot.install`. Any changes to the database schema must be accompanied by an update hook to ensure data integrity.
- **Batch Processing:** The module uses Drupal's Batch API to handle the potentially long-running process of taking a snapshot. This is the preferred method for triggering snapshots, both manually and via cron.

### Testing

This module does not have an established automated testing framework. There is no `tests` directory, and the local development environment is not configured for easy execution of standard tools like Drush. As a result, frontend verification has been consistently skipped. Manual testing is required for all changes.
