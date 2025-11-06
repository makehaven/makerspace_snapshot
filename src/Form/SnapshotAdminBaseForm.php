<?php

namespace Drupal\makerspace_snapshot\Form;

use Drupal\Component\Utility\Html;
use Drupal\Core\Database\Connection;
use Drupal\Core\Extension\ModuleHandlerInterface;
use Drupal\Core\Form\FormBase;
use Drupal\Core\Link;
use Drupal\Core\Url;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Shared utilities for snapshot admin forms.
 */
abstract class SnapshotAdminBaseForm extends FormBase {

  /**
   * Database connection.
   *
   * @var \Drupal\Core\Database\Connection
   */
  protected Connection $database;

  /**
   * Snapshot service.
   *
   * @var \Drupal\makerspace_snapshot\SnapshotService
   */
  protected SnapshotService $snapshotService;

  /**
   * Module handler service.
   *
   * @var \Drupal\Core\Extension\ModuleHandlerInterface
   */
  protected ModuleHandlerInterface $moduleHandler;

  /**
   * Constructs the base form.
   */
  public function __construct(Connection $database, SnapshotService $snapshot_service, ModuleHandlerInterface $module_handler) {
    $this->database = $database;
    $this->snapshotService = $snapshot_service;
    $this->moduleHandler = $module_handler;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database'),
      $container->get('makerspace_snapshot.snapshot_service'),
      $container->get('module_handler')
    );
  }

  /**
   * Builds a human-friendly label for a snapshot definition.
   */
  protected function formatDefinitionLabel(string $definition): string {
    $definitions = $this->snapshotService->buildDefinitions();
    if (isset($definitions[$definition]['label'])) {
      return (string) $this->t($definitions[$definition]['label']);
    }
    $label = str_replace('_', ' ', $definition);
    return ucwords($label);
  }

  /**
   * Builds a human-friendly label for a snapshot type.
   */
  protected function formatTypeLabel(string $type): string {
    $label = str_replace('_', ' ', $type);
    return ucwords($label);
  }

  /**
   * Builds a human-friendly label for a snapshot source.
   */
  protected function formatSourceLabel(string $source): string {
    $source = $source ?: 'system';
    $map = [
      'manual_form' => $this->t('Manual (Admin)'),
      'manual_import' => $this->t('Manual (Import)'),
      'manual_drush' => $this->t('Manual (Drush)'),
      'automatic_cron' => $this->t('Automatic (Cron)'),
      'system' => $this->t('System'),
    ];

    if (isset($map[$source])) {
      return $map[$source];
    }

    $label = str_replace('_', ' ', $source);
    return ucwords($label);
  }

  /**
   * Returns the list of definitions supported by manual snapshots.
   *
   * @return string[]
   *   An associative array keyed by definition machine name.
   */
  protected function getManualSnapshotDefinitionOptions(): array {
    $definitions = [];
    foreach ($this->snapshotService->buildDefinitions() as $definition_key => $info) {
      $acquisition = $info['acquisition'] ?? 'automated';
      if ($acquisition !== 'automated') {
        continue;
      }
      $definitions[$definition_key] = $this->formatDefinitionLabel($definition_key);
    }
    return $definitions;
  }

  /**
   * Formats a human-readable data source label.
   */
  protected function formatDataSourceLabel(string $source): string {
    switch (strtolower($source)) {
      case 'drupal sql':
        return (string) $this->t('Drupal SQL');

      case 'civicrm sql':
        return (string) $this->t('CiviCRM SQL');

      case 'manual import':
        return (string) $this->t('Manual Import');

      case 'external api':
        return (string) $this->t('External API');

      default:
        return (string) $this->t('@source', ['@source' => $source]);
    }
  }

  /**
   * Formats acquisition mode labels.
   */
  protected function formatAcquisitionLabel(string $acquisition): string {
    switch (strtolower($acquisition)) {
      case 'automated':
        return (string) $this->t('Automated');

      case 'import':
        return (string) $this->t('Manual Import');

      case 'placeholder':
        return (string) $this->t('Planned Integration');

      default:
        return (string) $this->t('@mode', ['@mode' => $acquisition]);
    }
  }

  /**
   * Formats schedule arrays into a comma-separated list.
   */
  protected function formatScheduleList(array $schedules): string {
    if (empty($schedules)) {
      return (string) $this->t('None');
    }
    $labels = array_map(function ($schedule) {
      return $this->formatTypeLabel((string) $schedule);
    }, $schedules);
    return implode(', ', $labels);
  }

  /**
   * Provides contextual messaging when SQL is unavailable.
   */
  protected function getAcquisitionPlaceholderMessage(string $acquisition): string {
    switch (strtolower($acquisition)) {
      case 'import':
        return (string) $this->t('This dataset is populated via manual import, so no SQL preview is available.');

      case 'placeholder':
        return (string) $this->t('This dataset is planned for a future integration. Data collection will begin once the source is wired.');

      default:
        return (string) $this->t('No automated SQL source has been implemented for this dataset yet.');
    }
  }

  /**
   * Builds dataset metadata sections used on admin pages.
   */
  protected function buildDatasetInformation(): array {
    $sections = [];

    $source_queries = $this->snapshotService->getSourceQueries();
    $dataset_map = $this->snapshotService->getDatasetSourceMap();
    $definition_metadata = $this->snapshotService->buildDefinitions();

    foreach ($dataset_map as $dataset_key => $dataset) {
      $dataset_section = [
        '#type' => 'details',
        '#title' => isset($definition_metadata[$dataset_key]['label'])
          ? $this->t($definition_metadata[$dataset_key]['label'])
          : $dataset['label'],
        '#open' => FALSE,
      ];

      if (!empty($dataset['description'])) {
        $dataset_section['description'] = [
          '#markup' => '<p>' . Html::escape($dataset['description']) . '</p>',
        ];
      }

      $definition_info = $definition_metadata[$dataset_key] ?? [];
      $meta_items = [];
      if (!empty($definition_info['data_source'])) {
        $meta_items[] = $this->t('Primary source: @source', ['@source' => $this->formatDataSourceLabel($definition_info['data_source'])]);
      }
      if (!empty($definition_info['acquisition'])) {
        $meta_items[] = $this->t('Acquisition: @mode', ['@mode' => $this->formatAcquisitionLabel($definition_info['acquisition'])]);
      }
      if (!empty($definition_info['schedules']) && is_array($definition_info['schedules'])) {
        $meta_items[] = $this->t('Default schedules: @list', ['@list' => $this->formatScheduleList($definition_info['schedules'])]);
      }

      if ($meta_items) {
        $dataset_section['metadata'] = [
          '#theme' => 'item_list',
          '#items' => $meta_items,
          '#attributes' => ['class' => ['snapshot-dataset-meta']],
        ];
      }

      $download_links = $this->buildHistoricalDownloadLinks($dataset_key);
      if (!empty($download_links)) {
        $dataset_section['historical_downloads'] = [
          '#type' => 'item',
          '#title' => $this->t('Historical downloads'),
          '#markup' => implode(' | ', $download_links),
          '#attributes' => ['class' => ['snapshot-dataset-downloads']],
        ];
      }

      $acquisition_mode = $definition_info['acquisition'] ?? 'automated';

      if ($acquisition_mode !== 'automated') {
        $dataset_section['placeholder'] = [
          '#markup' => '<p><em>' . $this->getAcquisitionPlaceholderMessage($acquisition_mode) . '</em></p>',
        ];
      }
      elseif (!empty($dataset['queries'])) {
        $dataset_section['queries'] = [
          '#type' => 'container',
          '#attributes' => ['class' => ['snapshot-dataset-queries']],
        ];

        foreach ($dataset['queries'] as $query_key) {
          if (!isset($source_queries[$query_key])) {
            continue;
          }
          $query_info = $source_queries[$query_key];
          $dataset_section['queries'][$query_key] = [
            '#type' => 'details',
            '#title' => $query_info['label'],
            '#open' => FALSE,
          ];

          if (!empty($query_info['description'])) {
            $dataset_section['queries'][$query_key]['description'] = [
              '#markup' => '<p>' . Html::escape($query_info['description']) . '</p>',
            ];
          }

          $dataset_section['queries'][$query_key]['sql'] = [
            '#type' => 'item',
            '#title' => $this->t('SQL'),
            '#markup' => '<pre><code>' . Html::escape(trim($query_info['sql'])) . '</code></pre>',
          ];
        }
      }
      elseif ($acquisition_mode === 'automated') {
        $dataset_section['placeholder'] = [
          '#markup' => '<p><em>' . $this->t('No automated SQL source has been implemented for this dataset yet.') . '</em></p>',
        ];
      }

      $sections['dataset_' . $dataset_key] = $dataset_section;
    }

    return $sections;
  }

  /**
   * Returns known routes for historical snapshot exports.
   *
   * @return array
   *   Associative array keyed by dataset definition.
   */
  protected function getHistoricalDownloadRouteMap(): array {
    $supported = [
      'membership_totals',
      'membership_activity',
      'plan_levels',
      'event_registrations',
      'membership_types',
      'donation_metrics',
      'event_type_metrics',
      'survey_metrics',
      'tool_availability',
    ];

    $definitions = $this->snapshotService->buildDefinitions();
    $map = [];
    foreach ($supported as $definition) {
      if (isset($definitions[$definition])) {
        $map[$definition] = 'makerspace_snapshot.download.historical';
      }
    }

    $this->moduleHandler->alter('makerspace_snapshot_historical_routes', $map);
    return $map;
  }

  /**
   * Builds historical download links for a dataset definition.
   *
   * @param string $definition
   *   Dataset definition machine name.
   *
   * @return string[]
   *   Rendered link strings.
   */
  protected function buildHistoricalDownloadLinks(string $definition): array {
    $route_map = $this->getHistoricalDownloadRouteMap();
    if (!isset($route_map[$definition])) {
      return [];
    }

    $route_name = $route_map[$definition];
    $links = [];
    $links[] = Link::fromTextAndUrl(
      $this->t('All periods'),
      Url::fromRoute($route_name, ['snapshot_definition' => $definition])
    )->toString();

    $available_types = $this->getAvailableSnapshotTypes([$definition]);
    foreach ($available_types[$definition] ?? [] as $type) {
      $links[] = Link::fromTextAndUrl(
        $this->formatTypeLabel($type),
        Url::fromRoute($route_name, ['snapshot_definition' => $definition], ['query' => ['type' => $type]])
      )->toString();
    }

    return $links;
  }

  /**
   * Determines available snapshot types per definition.
   *
   * @param string[] $definitions
   *   Definition keys to inspect.
   *
   * @return array
   *   Array keyed by definition with distinct snapshot types.
   */
  protected function getAvailableSnapshotTypes(array $definitions): array {
    $types = [];
    if (empty($definitions)) {
      return $types;
    }

    $has_snapshot_table = $this->database->schema()->tableExists('ms_snapshot');
    if (!$has_snapshot_table) {
      return $types;
    }

    $query = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['definition', 'snapshot_type'])
      ->condition('definition', $definitions, 'IN')
      ->isNotNull('snapshot_type')
      ->distinct()
      ->orderBy('definition', 'ASC')
      ->orderBy('snapshot_type', 'ASC');

    foreach ($query->execute() as $record) {
      $definition = (string) $record->definition;
      $type = (string) $record->snapshot_type;
      if ($definition === '' || $type === '') {
        continue;
      }
      $types[$definition][] = $type;
    }

    foreach ($types as &$type_list) {
      $type_list = array_values(array_unique($type_list));
    }

    return $types;
  }

}
