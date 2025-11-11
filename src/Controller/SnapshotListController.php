<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Database\Connection;
use Drupal\Core\Url;
use Drupal\makerspace_snapshot\Form\SnapshotListFilterForm;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Controller for the snapshot listing page.
 */
class SnapshotListController extends ControllerBase {

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
   * Constructs the controller.
   */
  public function __construct(Connection $database, SnapshotService $snapshot_service) {
    $this->database = $database;
    $this->snapshotService = $snapshot_service;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('database'),
      $container->get('makerspace_snapshot.snapshot_service')
    );
  }

  /**
   * Snapshot listing page.
   */
  public function listing() {
    $filters = $this->getFiltersFromRequest();
    [$type_options, $definition_options, $date_options] = $this->buildFilterOptions($filters);

    $build['filters'] = $this->formBuilder()->getForm(
      SnapshotListFilterForm::class,
      [
        'type_options' => $type_options,
        'definition_options' => $definition_options,
        'date_options' => $date_options,
        'current_filters' => $filters,
      ]
    );

    $build['table'] = [
      '#type' => 'table',
      '#header' => $this->buildHeader(),
      '#rows' => $this->buildRows($filters),
      '#empty' => $this->t('No snapshots found.'),
    ];

    return $build;
  }

  /**
   * Builds table header.
   */
  protected function buildHeader(): array {
    return [
      'snapshot_id' => $this->t('ID'),
      'definition' => $this->t('Definition'),
      'snapshot_type' => $this->t('Type'),
      'snapshot_date' => $this->t('Date'),
      'members_total' => $this->t('Members'),
      'source' => $this->t('Source'),
      'created_at' => $this->t('Created'),
      'operations' => $this->t('Operations'),
    ];
  }

  /**
   * Builds table rows.
   */
  protected function buildRows(array $filters): array {
    $rows = [];
    $query = $this->database->select('ms_snapshot', 's')
      ->fields('s')
      ->orderBy('snapshot_date', 'DESC')
      ->orderBy('created_at', 'DESC');

    if (!empty($filters['type'])) {
      $query->condition('snapshot_type', $filters['type']);
    }
    if (!empty($filters['date'])) {
      $query->condition('snapshot_date', $filters['date']);
    }
    if (!empty($filters['definition'])) {
      $query->condition('definition', $filters['definition']);
    }

    $snapshots = $query->execute()->fetchAll();
    if (empty($snapshots)) {
      return [];
    }

    $snapshotIds = array_map(static function ($snapshot) {
      return (int) $snapshot->id;
    }, $snapshots);

    $orgTotals = [];
    $typeTotals = [];
    if (!empty($snapshotIds)) {
      $orgResult = $this->database->select('ms_fact_org_snapshot', 'o')
        ->fields('o', ['snapshot_id', 'members_total'])
        ->condition('snapshot_id', $snapshotIds, 'IN')
        ->execute();
      foreach ($orgResult as $row) {
        $orgTotals[(int) $row->snapshot_id] = (int) $row->members_total;
      }

      $typeResult = $this->database->select('ms_fact_membership_type_snapshot', 'mt')
        ->fields('mt', ['snapshot_id', 'members_total'])
        ->condition('snapshot_id', $snapshotIds, 'IN')
        ->execute();
      foreach ($typeResult as $row) {
        $sid = (int) $row->snapshot_id;
        if (!isset($typeTotals[$sid])) {
          $typeTotals[$sid] = (int) $row->members_total;
        }
      }
    }

    foreach ($snapshots as $snapshot) {
      $definition_key = $snapshot->definition ?? 'membership_totals';
      $operations = $this->buildOperations($snapshot->id, $definition_key, $filters);

      $membersTotalValue = '';
      if ($definition_key === 'membership_totals' && isset($orgTotals[$snapshot->id])) {
        $membersTotalValue = $orgTotals[$snapshot->id];
      }
      elseif ($definition_key === 'membership_types' && isset($typeTotals[$snapshot->id])) {
        $membersTotalValue = $typeTotals[$snapshot->id];
      }

      $rows[] = [
        'snapshot_id' => $snapshot->id,
        'definition' => $this->formatDefinitionLabel($definition_key),
        'snapshot_type' => $snapshot->snapshot_type,
        'snapshot_date' => $snapshot->snapshot_date,
        'members_total' => $membersTotalValue,
        'source' => $this->formatSourceLabel($snapshot->source ?? ''),
        'created_at' => date('Y-m-d H:i:s', $snapshot->created_at),
        'operations' => [
          'data' => [
            '#type' => 'operations',
            '#links' => $operations,
          ],
        ],
      ];
    }

    return $rows;
  }

  /**
   * Builds operation links for a row.
   */
  protected function buildOperations(int $snapshot_id, string $definition, array $filters): array {
    $links = [];

    if ($download_url = $this->getDownloadUrl($snapshot_id, $definition)) {
      $links['download'] = [
        'title' => $this->t('Download CSV'),
        'url' => $download_url,
      ];
    }

    $links['delete'] = [
      'title' => $this->t('Delete'),
      'url' => Url::fromRoute('makerspace_snapshot.snapshot_delete', [
        'snapshot_id' => $snapshot_id,
      ], [
        'query' => array_filter($filters),
      ]),
    ];

    return $links;
  }

  /**
   * Determines download URL for a snapshot definition.
   */
  protected function getDownloadUrl(int $snapshot_id, string $definition) {
    switch ($definition) {
      case 'membership_totals':
        return Url::fromRoute('makerspace_snapshot.download.org_level', ['snapshot_id' => $snapshot_id]);

      case 'plan_levels':
        return Url::fromRoute('makerspace_snapshot.download.plan_level', ['snapshot_id' => $snapshot_id]);

      case 'membership_types':
        return Url::fromRoute('makerspace_snapshot.download.membership_types', ['snapshot_id' => $snapshot_id]);
      case 'membership_type_joins':
        return Url::fromRoute('makerspace_snapshot.download.membership_type_joins', ['snapshot_id' => $snapshot_id]);
      case 'membership_type_cancels':
        return Url::fromRoute('makerspace_snapshot.download.membership_type_cancels', ['snapshot_id' => $snapshot_id]);

      case 'event_registrations':
        return Url::fromRoute('makerspace_snapshot.download.event_registrations', ['snapshot_id' => $snapshot_id]);

      case 'donation_metrics':
        return Url::fromRoute('makerspace_snapshot.download.donation_metrics', ['snapshot_id' => $snapshot_id]);
      case 'donation_range_metrics':
        return Url::fromRoute('makerspace_snapshot.download.donation_range_metrics', ['snapshot_id' => $snapshot_id]);

      case 'event_type_metrics':
        return Url::fromRoute('makerspace_snapshot.download.event_type_metrics', ['snapshot_id' => $snapshot_id]);
      case 'event_type_counts':
        return Url::fromRoute('makerspace_snapshot.download.event_type_counts', ['snapshot_id' => $snapshot_id]);
      case 'event_type_registrations':
        return Url::fromRoute('makerspace_snapshot.download.event_type_registrations', ['snapshot_id' => $snapshot_id]);
      case 'event_type_revenue':
        return Url::fromRoute('makerspace_snapshot.download.event_type_revenue', ['snapshot_id' => $snapshot_id]);

      case 'survey_metrics':
        return Url::fromRoute('makerspace_snapshot.download.survey_metrics', ['snapshot_id' => $snapshot_id]);

      case 'tool_availability':
        return Url::fromRoute('makerspace_snapshot.download.tool_availability', ['snapshot_id' => $snapshot_id]);

      default:
        return NULL;
    }
  }

  /**
   * Builds filter options for the filter form.
   */
  protected function buildFilterOptions(array $filters): array {
    $type_options = [
      '' => $this->t('- All types -'),
      'monthly' => $this->t('Monthly'),
      'quarterly' => $this->t('Quarterly'),
      'annually' => $this->t('Annually'),
      'daily' => $this->t('Daily'),
    ];

    $definition_options = ['' => $this->t('- All definitions -')];
    $date_options = ['' => $this->t('- All dates -')];

    $has_snapshot_table = $this->database->schema()->tableExists('ms_snapshot');
    if (!$has_snapshot_table) {
      return [$type_options, $definition_options, $date_options];
    }

    $known_definitions = $this->snapshotService->buildDefinitions();
    foreach ($known_definitions as $definition_key => $info) {
      $definition_options[$definition_key] = $this->formatDefinitionLabel($definition_key);
    }

    $period_values = $this->database->select('ms_snapshot', 'p')
      ->fields('p', ['snapshot_date'])
      ->distinct()
      ->orderBy('snapshot_date', 'DESC')
      ->execute()
      ->fetchCol();

    $type_values = $this->database->select('ms_snapshot', 't')
      ->fields('t', ['snapshot_type'])
      ->distinct()
      ->execute()
      ->fetchCol();

    $definition_values = $this->database->select('ms_snapshot', 'd')
      ->fields('d', ['definition'])
      ->distinct()
      ->execute()
      ->fetchCol();

    foreach ($period_values as $value) {
      if (!$value) {
        continue;
      }
      $date_options[$value] = $this->formatSnapshotDate($value);
    }

    foreach ($type_values as $value) {
      if ($value === '' || isset($type_options[$value])) {
        continue;
      }
      $type_options[$value] = $this->formatTypeLabel($value);
    }

    foreach ($definition_values as $value) {
      if ($value === '' || isset($definition_options[$value])) {
        continue;
      }
      $definition_options[$value] = $this->formatDefinitionLabel($value);
    }

    if (!empty($filters['date']) && !isset($date_options[$filters['date']])) {
      $date_options[$filters['date']] = $this->formatSnapshotDate($filters['date']);
    }
    if (!empty($filters['type']) && !isset($type_options[$filters['type']])) {
      $type_options[$filters['type']] = $this->formatTypeLabel($filters['type']);
    }
    if (!empty($filters['definition']) && !isset($definition_options[$filters['definition']])) {
      $definition_options[$filters['definition']] = $this->formatDefinitionLabel($filters['definition']);
    }

    return [$type_options, $definition_options, $date_options];
  }

  /**
   * Reads filters from the current request.
   */
  protected function getFiltersFromRequest(): array {
    $request = \Drupal::request();
    $filters = [
      'type' => (string) $request->query->get('type', ''),
      'definition' => (string) $request->query->get('definition', ''),
      'date' => (string) $request->query->get('date', ''),
    ];

    return $this->normalizeFilters($filters);
  }

  /**
   * Normalizes filter arrays to expected keys.
   */
  protected function normalizeFilters(array $filters): array {
    return [
      'type' => isset($filters['type']) ? (string) $filters['type'] : '',
      'definition' => isset($filters['definition']) ? (string) $filters['definition'] : '',
      'date' => isset($filters['date']) ? (string) $filters['date'] : '',
    ];
  }

  /**
   * Formats snapshot dates for display.
   */
  protected function formatSnapshotDate(string $value): string {
    try {
      return (new \DateTimeImmutable($value))->format('F Y');
    }
    catch (\Exception $e) {
      return $value;
    }
  }

  /**
   * Formats definition label.
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
   * Formats snapshot type label.
   */
  protected function formatTypeLabel(string $type): string {
    $label = str_replace('_', ' ', $type);
    return ucwords($label);
  }

  /**
   * Formats snapshot source label.
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

}
