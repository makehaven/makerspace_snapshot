<?php

namespace Drupal\makerspace_snapshot\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Database\Connection;
use Drupal\makerspace_snapshot\SnapshotService;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\StreamedResponse;

/**
 * Provides historical CSV exports for snapshot datasets.
 */
class SnapshotHistoricalDownloadController extends ControllerBase {

  /**
   * Database connection.
   */
  protected Connection $database;

  /**
   * Snapshot service.
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
  public static function create(ContainerInterface $container): self {
    return new static(
      $container->get('database'),
      $container->get('makerspace_snapshot.snapshot_service')
    );
  }

  /**
   * Backwards-compatible entry point for org-level downloads.
   */
  public function downloadOrgLevelData(string $snapshot_definition): StreamedResponse {
    return $this->downloadHistoricalData($snapshot_definition ?: 'membership_totals');
  }

  /**
   * Backwards-compatible entry point for plan-level downloads.
   */
  public function downloadPlanLevelData(string $snapshot_definition): StreamedResponse {
    return $this->downloadHistoricalData($snapshot_definition ?: 'plan_levels');
  }

  /**
   * Streams historical data for any supported dataset definition.
   */
  public function downloadHistoricalData(string $snapshot_definition): StreamedResponse {
    $definition = $this->canonicalDefinition($snapshot_definition);
    $fallbacks = $this->getHeaderFallbacks();
    if (!isset($fallbacks[$definition])) {
      throw $this->createNotFoundException();
    }

    $snapshot_type = $this->getSnapshotTypeFilter();
    $headers = $this->resolveHeaders($definition, $fallbacks[$definition]);
    $filename = $this->buildFilename($definition, $snapshot_type);

    $label_row = $this->snapshotService->getDatasetLabelRow($definition, $headers);

    $response = new StreamedResponse(function () use ($definition, $snapshot_type, $headers, $label_row) {
      $handle = fopen('php://output', 'w');
      fputcsv($handle, $headers);
      if ($this->hasLabelRowContent($label_row)) {
        fputcsv($handle, $label_row);
      }
      $this->streamHistoricalRows($handle, $definition, $snapshot_type, $headers);
      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

    return $response;
  }

  /**
   * Streams rows for the requested dataset definition.
   */
  protected function streamHistoricalRows($handle, string $definition, string $snapshot_type, array $headers): void {
    $query = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_type', 'snapshot_date'])
      ->condition('definition', $definition);
    if ($snapshot_type !== '') {
      $query->condition('snapshot_type', $snapshot_type);
    }
    $query->orderBy('snapshot_date', 'ASC');
    $query->orderBy('snapshot_type', 'ASC');

    foreach ($query->execute()->fetchAll(\PDO::FETCH_ASSOC) as $record) {
      $type = (string) ($record['snapshot_type'] ?? '');
      $date = (string) ($record['snapshot_date'] ?? '');
      if ($date === '') {
        continue;
      }
      $export = $this->snapshotService->getSnapshotExportData($type, $date);
      $dataset = $export[$definition] ?? NULL;
      if (empty($dataset) || empty($dataset['rows'])) {
        continue;
      }
      foreach ($dataset['rows'] as $row) {
        fputcsv($handle, $this->normalizeDatasetRow($row, $headers));
      }
    }
  }

  /**
   * Normalizes dataset rows to match the header length/order.
   */
  protected function normalizeDatasetRow(array $row, array $headers): array {
    if (array_keys($row) === range(0, count($row) - 1)) {
      $ordered = $row;
    }
    else {
      $ordered = [];
      foreach ($headers as $column) {
        $ordered[] = $row[$column] ?? '';
      }
    }
    $missing = count($headers) - count($ordered);
    if ($missing > 0) {
      $ordered = array_merge($ordered, array_fill(0, $missing, ''));
    }
    return $ordered;
  }

  /**
   * Determines whether a label row contains usable values.
   */
  protected function hasLabelRowContent(array $row): bool {
    foreach ($row as $value) {
      if ($value !== '' && $value !== NULL) {
        return TRUE;
      }
    }
    return FALSE;
  }

  /**
   * Normalizes snapshot definition aliases to canonical machine names.
   */
  protected function canonicalDefinition(string $definition): string {
    $map = [
      'org' => 'membership_totals',
      'org_level' => 'membership_totals',
      'plan' => 'plan_levels',
      'plan_level' => 'plan_levels',
    ];
    $key = strtolower($definition);
    return $map[$key] ?? $key;
  }

  /**
   * Builds a CSV filename with optional snapshot type context.
   */
  protected function buildFilename(string $definition, string $snapshot_type): string {
    $suffix = $snapshot_type !== '' ? $snapshot_type . '_historical' : 'historical';
    return sprintf('%s_%s.csv', $definition, $suffix);
  }

  /**
   * Returns the requested snapshot type filter.
   */
  protected function getSnapshotTypeFilter(): string {
    $type = (string) \Drupal::request()->query->get('type', '');
    return trim($type);
  }

  /**
   * Resolves dataset headers using snapshot definitions.
   *
   * @param string $definition
   *   The dataset definition key.
   * @param string[] $defaults
   *   Default headers to use if the definition is missing.
   *
   * @return string[]
   *   Header values with snapshot_date guaranteed to lead.
   */
  protected function resolveHeaders(string $definition, array $defaults): array {
    $definitions = $this->snapshotService->buildDefinitions();
    $headers = $definitions[$definition]['headers'] ?? $defaults;
    $headers = array_values(array_unique($headers));
    if (!in_array('snapshot_date', $headers, TRUE)) {
      array_unshift($headers, 'snapshot_date');
    }
    return $headers;
  }

  /**
   * Provides fallback headers for known datasets.
   */
  protected function getHeaderFallbacks(): array {
    return [
      'membership_totals' => [
        'snapshot_date',
        'members_active',
        'members_paused',
        'members_lapsed',
        'members_total',
      ],
      'plan_levels' => [
        'snapshot_date',
      ],
      'event_registrations' => [
        'snapshot_date',
        'event_id',
        'event_title',
        'event_start_date',
        'registration_count',
      ],
      'membership_types' => [
        'snapshot_date',
        'members_total',
      ],
      'membership_type_joins' => [
        'snapshot_date',
        'joins_total',
      ],
      'membership_type_cancels' => [
        'snapshot_date',
        'cancels_total',
      ],
      'donation_metrics' => [
        'snapshot_date',
        'period_year',
        'period_month',
        'donors_count',
        'ytd_unique_donors',
        'contributions_count',
        'recurring_contributions_count',
        'onetime_contributions_count',
        'recurring_donors_count',
        'onetime_donors_count',
        'first_time_donors_count',
        'total_amount',
        'recurring_amount',
        'onetime_amount',
      ],
      'donation_range_metrics' => [
        'snapshot_date',
        'period_year',
        'period_month',
        'is_year_to_date',
        'range_key',
        'range_label',
        'min_amount',
        'max_amount',
        'donors_count',
        'contributions_count',
        'total_amount',
      ],
      'event_type_metrics' => [
        'snapshot_date',
        'period_year',
        'period_quarter',
        'period_month',
        'event_type_id',
        'event_type_label',
        'events_count',
        'participant_count',
        'total_amount',
        'average_ticket',
      ],
      'event_type_counts' => [
        'snapshot_date',
      ],
      'event_type_registrations' => [
        'snapshot_date',
      ],
      'event_type_revenue' => [
        'snapshot_date',
      ],
      'survey_metrics' => [
        'snapshot_date',
        'respondents_count',
        'likely_recommend',
        'net_promoter_score',
        'satisfaction_rating',
        'equipment_score',
        'learning_resources_score',
        'member_events_score',
        'paid_workshops_score',
        'facility_score',
        'community_score',
        'vibe_score',
      ],
      'tool_availability' => [
        'snapshot_date',
        'period_year',
        'period_month',
        'period_day',
        'total_tools',
        'available_tools',
        'down_tools',
        'maintenance_tools',
        'unknown_tools',
        'availability_percent',
      ],
    ];
  }

}
