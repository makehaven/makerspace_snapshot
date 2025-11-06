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
   * Streams historical membership total data as CSV.
   */
  public function downloadOrgLevelData(string $snapshot_definition): StreamedResponse {
    $definition = $this->normalizeDefinition($snapshot_definition, 'membership_totals');
    if ($definition !== 'membership_totals') {
      throw $this->createNotFoundException();
    }
    $snapshot_type = $this->getSnapshotTypeFilter();
    $headers = $this->resolveHeaders($definition, [
      'snapshot_date',
      'members_active',
      'members_paused',
      'members_lapsed',
      'members_total',
    ]);

    $response = new StreamedResponse(function () use ($definition, $snapshot_type, $headers) {
      $handle = fopen('php://output', 'w');
      fputcsv($handle, $headers);

      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_org_snapshot', 'o', 's.id = o.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('o', ['members_active', 'members_paused', 'members_lapsed', 'members_total']);
      $query->condition('s.definition', $definition);
      if ($snapshot_type !== '') {
        $query->condition('s.snapshot_type', $snapshot_type);
      }
      $query->orderBy('s.snapshot_date', 'ASC');

      foreach ($query->execute()->fetchAll(\PDO::FETCH_ASSOC) as $row) {
        $row = array_map(static function ($value) {
          return is_numeric($value) ? (int) $value : $value;
        }, $row);
        $ordered = [];
        foreach ($headers as $column) {
          $ordered[] = $row[$column] ?? '';
        }
        fputcsv($handle, $ordered);
      }

      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $this->buildFilename($definition, $snapshot_type)));

    return $response;
  }

  /**
   * Streams historical per-plan member data as CSV.
   */
  public function downloadPlanLevelData(string $snapshot_definition): StreamedResponse {
    $definition = $this->normalizeDefinition($snapshot_definition, 'plan_levels');
    if ($definition !== 'plan_levels') {
      throw $this->createNotFoundException();
    }
    $snapshot_type = $this->getSnapshotTypeFilter();
    $headers = $this->resolveHeaders($definition, [
      'snapshot_date',
      'plan_code',
      'plan_label',
      'count_members',
    ]);

    $response = new StreamedResponse(function () use ($definition, $snapshot_type, $headers) {
      $handle = fopen('php://output', 'w');
      fputcsv($handle, $headers);

      $query = $this->database->select('ms_snapshot', 's');
      $query->join('ms_fact_plan_snapshot', 'p', 's.id = p.snapshot_id');
      $query->fields('s', ['snapshot_date']);
      $query->fields('p', ['plan_code', 'plan_label', 'count_members']);
      $query->condition('s.definition', $definition);
      if ($snapshot_type !== '') {
        $query->condition('s.snapshot_type', $snapshot_type);
      }
      $query->orderBy('s.snapshot_date', 'ASC');
      $query->orderBy('p.plan_code', 'ASC');

      foreach ($query->execute()->fetchAll(\PDO::FETCH_ASSOC) as $row) {
        $row['plan_label'] = (string) $row['plan_label'];
        $row['count_members'] = (int) $row['count_members'];
        $ordered = [];
        foreach ($headers as $column) {
          $ordered[] = $row[$column] ?? '';
        }
        fputcsv($handle, $ordered);
      }

      fclose($handle);
    });

    $response->headers->set('Content-Type', 'text/csv');
    $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $this->buildFilename($definition, $snapshot_type)));

    return $response;
  }

  /**
   * Normalizes snapshot definition aliases.
   */
  protected function normalizeDefinition(string $definition, string $expected): string {
    $map = [
      'org' => 'membership_totals',
      'org_level' => 'membership_totals',
      'plan' => 'plan_levels',
      'plan_level' => 'plan_levels',
    ];
    $key = strtolower($definition);
    if (isset($map[$key])) {
      return $map[$key] === $expected ? $map[$key] : $definition;
    }
    if ($key === $expected) {
      return $expected;
    }
    return $definition;
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

}
