<?php

namespace Drupal\makerspace_snapshot;

use Psr\Log\LoggerInterface;
use Drupal\Core\Cache\Cache;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\Extension\ModuleHandlerInterface;

/**
 * Service description.
 */
class SnapshotService {

  /**
   * The database connection.
   *
   * @var \Drupal\Core\Database\Connection
   */
  protected $database;

  /**
   * The logger.
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * The config factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;

  /**
   * The module handler.
   *
   * @var \Drupal\Core\Extension\ModuleHandlerInterface
   */
  protected ModuleHandlerInterface $moduleHandler;

  /**
   * Canonical SQL queries used to build snapshots.
   *
   * @var array
   */
  protected array $sourceQueries = [
    'sql_active' => [
      'label' => 'Active members',
      'description' => 'Returns the current active membership roster with plan codes and labels. Used for membership totals and plan-level counts.',
      'sql' => <<<SQL
SELECT u.uid AS member_id,
       'MEMBER' AS plan_code,
       'Member' AS plan_label
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id AND r.roles_target_id = 'member'
LEFT JOIN user__field_chargebee_payment_pause cb_pause ON cb_pause.entity_id = u.uid AND cb_pause.deleted = 0
LEFT JOIN user__field_manual_pause manual_pause ON manual_pause.entity_id = u.uid AND manual_pause.deleted = 0
WHERE u.status = 1
  AND COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 0
  AND COALESCE(manual_pause.field_manual_pause_value, 0) = 0
SQL,
    ],
    'sql_paused' => [
      'label' => 'Paused members',
      'description' => 'Lists members flagged as paused so they can be counted separately from active.',
      'sql' => <<<SQL
SELECT u.uid AS member_id,
       'MEMBER_PAUSED' AS plan_code,
       'Member (Paused)' AS plan_label
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id AND r.roles_target_id = 'member'
LEFT JOIN user__field_chargebee_payment_pause cb_pause ON cb_pause.entity_id = u.uid AND cb_pause.deleted = 0
LEFT JOIN user__field_manual_pause manual_pause ON manual_pause.entity_id = u.uid AND manual_pause.deleted = 0
WHERE u.status = 1
  AND (
    COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 1
    OR COALESCE(manual_pause.field_manual_pause_value, 0) = 1
  )
SQL,
    ],
    'sql_lapsed' => [
      'label' => 'Lapsed members',
      'description' => 'Identifies users without an active membership role to track lapsed counts.',
      'sql' => <<<SQL
SELECT u.uid AS member_id,
       'MEMBER_LAPSED' AS plan_code,
       'Member (Lapsed)' AS plan_label
FROM users_field_data u
WHERE u.uid NOT IN (
  SELECT entity_id FROM user__roles WHERE roles_target_id = 'member'
)
SQL,
    ],
    'sql_joins' => [
      'label' => 'Joins in period',
      'description' => 'Finds members who joined during the reporting window. Requires :start and :end parameters.',
      'sql' => <<<SQL
SELECT u.uid AS member_id,
       'MEMBER' AS plan_code,
       'Member' AS plan_label,
       FROM_UNIXTIME(u.created, '%Y-%m-%d') AS occurred_at
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id
WHERE r.roles_target_id = 'member'
  AND u.created BETWEEN UNIX_TIMESTAMP(:start) AND UNIX_TIMESTAMP(:end)
SQL,
    ],
    'sql_cancels' => [
      'label' => 'Cancels in period',
      'description' => 'Placeholder query for cancellations; replace with site-specific logic that records membership cancellations between :start and :end.',
      'sql' => <<<SQL
SELECT NULL AS member_id,
       NULL AS plan_code,
       NULL AS plan_label,
       NULL AS occurred_at
WHERE 1 = 0
SQL,
    ],
  ];

  /**
   * Dataset definitions mapped to the queries that power them.
   *
   * @var array
   */
  protected array $datasetSourceMap = [
    'membership_totals' => [
      'label' => 'Membership Totals',
      'description' => 'Aggregates active, paused, and lapsed member counts for the reporting period.',
      'queries' => ['sql_active', 'sql_paused', 'sql_lapsed', 'sql_joins', 'sql_cancels'],
    ],
    'membership_activity' => [
      'label' => 'Membership Activity',
      'description' => 'Calculates joins, cancels, and net change from the period-specific queries.',
      'queries' => ['sql_joins', 'sql_cancels'],
    ],
    'plan_levels' => [
      'label' => 'Plan Levels',
      'description' => 'Counts active members by plan code using the active member snapshot.',
      'queries' => ['sql_active'],
    ],
    'donation_metrics' => [
      'label' => 'Donation Metrics',
      'description' => 'Aggregates donor and contribution metrics for the reporting period.',
      'queries' => [],
    ],
    'event_type_metrics' => [
      'label' => 'Event Type Metrics',
      'description' => 'Summarizes counted event registrations and revenue by event type.',
      'queries' => [],
    ],
    'survey_metrics' => [
      'label' => 'Annual Survey Metrics',
      'description' => 'Stores imported satisfaction and recommendation scores from the annual member survey.',
      'queries' => [],
    ],
    'tool_availability' => [
      'label' => 'Tool Availability Metrics',
      'description' => 'Placeholder entry for future asset_status-driven uptime calculations.',
      'queries' => [],
    ],
    'event_registrations' => [
      'label' => 'Event Registrations',
      'description' => 'Currently populated via CSV import. No automated SQL source has been implemented yet.',
      'queries' => [],
    ],
  ];

  /**
   * Dataset metadata keyed by definition machine name.
   */
  protected array $datasetDefinitions = [
    'membership_totals' => [
      'label' => 'Membership Totals',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'members_active', 'members_paused', 'members_lapsed'],
      'dataset_type' => 'membership_totals',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'membership_activity' => [
      'label' => 'Membership Activity',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'joins', 'cancels', 'net_change'],
      'dataset_type' => 'membership_activity',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'donation_metrics' => [
      'label' => 'Donation Metrics',
      'schedules' => ['monthly'],
      'headers' => [
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
        'total_amount',
        'recurring_amount',
        'onetime_amount',
      ],
      'dataset_type' => 'donation_metrics',
      'acquisition' => 'automated',
      'data_source' => 'CiviCRM SQL',
    ],
    'event_registrations' => [
      'label' => 'Event Registrations',
      'schedules' => ['daily'],
      'headers' => ['snapshot_date', 'event_id', 'event_title', 'event_start_date', 'registration_count'],
      'dataset_type' => 'event_registrations',
      'acquisition' => 'import',
      'data_source' => 'Manual Import',
    ],
    'plan_levels' => [
      'label' => 'Membership Plan Levels',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'plan_code', 'plan_label', 'count_members'],
      'dataset_type' => 'plan_levels',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'event_type_metrics' => [
      'label' => 'Event Type Metrics',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => [
        'snapshot_date',
        'period_year',
        'period_quarter',
        'period_month',
        'event_type_id',
        'event_type_label',
        'participant_count',
        'total_amount',
        'average_ticket',
      ],
      'dataset_type' => 'event_type_metrics',
      'acquisition' => 'automated',
      'data_source' => 'CiviCRM SQL',
    ],
    'survey_metrics' => [
      'label' => 'Member Survey Metrics',
      'schedules' => ['annually'],
      'headers' => [
        'snapshot_date',
        'period_year',
        'period_month',
        'period_day',
        'timeframe_label',
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
      'dataset_type' => 'survey_metrics',
      'acquisition' => 'import',
      'data_source' => 'Manual Import',
    ],
    'tool_availability' => [
      'label' => 'Tool Availability Metrics',
      'schedules' => ['daily', 'monthly'],
      'headers' => [
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
      'dataset_type' => 'tool_availability',
      'acquisition' => 'placeholder',
      'data_source' => 'External API',
    ],
  ];

  /**
   * Constructs a new SnapshotService object.
   *
   * @param \Drupal\Core\Database\Connection $database
   *   The database connection.
   * @param \Psr\Log\LoggerInterface $logger
   *   The logger.
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   *   The config factory.
   * @param \Drupal\Core\Extension\ModuleHandlerInterface $module_handler
   *   The module handler.
   */
  public function __construct(Connection $database, LoggerInterface $logger, ConfigFactoryInterface $config_factory, ?ModuleHandlerInterface $module_handler = NULL) {
    $this->database = $database;
    $this->logger = $logger;
    $this->configFactory = $config_factory;
    $this->moduleHandler = $module_handler ?? \Drupal::moduleHandler();
  }

  public function buildDefinitions() {
    return $this->datasetDefinitions;
  }

  /**
   * Takes a snapshot.
   *
   * @param string $snapshot_type
   *   The cadence of the snapshot (monthly, quarterly, etc.).
   * @param bool $is_test
   *   Whether the snapshot is a test snapshot.
   * @param string|null $snapshot_date
   *   The date associated with the snapshot (Y-m-d format).
   * @param string $source
   *   The origin of the snapshot (manual_form, manual_import, manual_drush, automatic_cron, system).
   * @param string[]|null $definitions
   *   Optional list of definitions to update. Defaults to all supported definitions.
   */
  public function takeSnapshot($snapshot_type, $is_test = FALSE, $snapshot_date = NULL, string $source = 'system', ?array $definitions = NULL) {
    try {
      $isTest = (bool) $is_test;

      $snapshotDate = $snapshot_date ?? (new \DateTime())->format('Y-m-d');
      $snapshotDate = (new \DateTimeImmutable($snapshotDate))->format('Y-m-01');

      $periodBounds = $this->resolvePeriodBounds(new \DateTimeImmutable($snapshotDate), $snapshot_type);
      $periodStartObject = $periodBounds['start'];
      $periodEndObject = $periodBounds['end'];
      $periodStart = $periodStartObject->format('Y-m-d H:i:s');
      $periodEnd = $periodEndObject->format('Y-m-d H:i:s');

      $supportedDefinitions = array_keys($this->datasetDefinitions);
      $requestedDefinitions = $definitions ?? $supportedDefinitions;
      $selectedDefinitions = array_values(array_intersect($supportedDefinitions, $requestedDefinitions));
      if (empty($selectedDefinitions)) {
        $selectedDefinitions = $supportedDefinitions;
      }

      $isSystemRun = ($source === 'system');
      if ($isSystemRun) {
        $selectedDefinitions = array_values(array_filter($selectedDefinitions, function (string $definition): bool {
          return ($this->datasetDefinitions[$definition]['acquisition'] ?? 'automated') === 'automated';
        }));
      }

      if (empty($selectedDefinitions)) {
        $this->logger->warning('No eligible dataset definitions selected for snapshot type @type using source @source.', ['@type' => $snapshot_type, '@source' => $source]);
        return;
      }

      $sourceQueries = $this->getSourceQueries();

      $stateQuery = function(string $key) use ($sourceQueries): array {
        $sql = $sourceQueries[$key]['sql'] ?? '';
        if (!$sql) {
          return [];
        }
        $rows = $this->database->query($sql)->fetchAll(\PDO::FETCH_ASSOC);
        return array_map(static function ($r) {
          return [
            'member_id' => (string) $r['member_id'],
            'plan_code' => (string) $r['plan_code'],
            'plan_label' => (string) ($r['plan_label'] ?? $r['plan_code']),
          ];
        }, $rows);
      };

      $periodQuery = function(string $key, string $start, string $end) use ($sourceQueries): array {
        $sql = $sourceQueries[$key]['sql'] ?? '';
        if (!$sql) {
          return [];
        }

        $params = [];
        if (strpos($sql, ':start') !== FALSE && strpos($sql, ':end') !== FALSE) {
          $params = [':start' => $start, ':end' => $end];
        }

        return $this->database->query($sql, $params)->fetchAll(\PDO::FETCH_ASSOC);
      };

      $active  = $stateQuery('sql_active');
      $paused  = $stateQuery('sql_paused');
      $lapsed  = $stateQuery('sql_lapsed');
      $joins   = $periodQuery('sql_joins',   $periodStart, $periodEnd);
      $cancels = $periodQuery('sql_cancels', $periodStart, $periodEnd);

      $members_active = count($active);
      $members_paused = count($paused);
      $members_lapsed = count($lapsed);
      $joins_count    = count($joins);
      $cancels_count  = count($cancels);
      $net_change     = $joins_count - $cancels_count;

      // Create snapshot metadata rows per definition.
      $snapshotIds = [];
      foreach ($selectedDefinitions as $definition) {
        $snapshot_id = $this->database->insert('ms_snapshot')
          ->fields([
            'definition'    => $definition,
            'snapshot_type' => $snapshot_type,
            'snapshot_date' => $snapshotDate,
            'source'        => $source,
            'created_at'    => time(),
          ])->execute();

        if (!$snapshot_id) {
          $this->logger->error("Failed to create {$definition} snapshot for {$snapshotDate}");
          continue;
        }
        $snapshotIds[$definition] = $snapshot_id;
      }

      if (empty($snapshotIds)) {
        $this->logger->error("Failed to create any snapshot records for {$snapshotDate}");
        return;
      }

      if (isset($snapshotIds['membership_totals'])) {
        $this->database->insert('ms_fact_org_snapshot')
          ->fields([
            'snapshot_id'    => $snapshotIds['membership_totals'],
            'members_active' => $members_active,
            'members_paused' => $members_paused,
            'members_lapsed' => $members_lapsed,
            'joins'          => 0,
            'cancels'        => 0,
            'net_change'     => 0,
          ])->execute();
      }

      if (isset($snapshotIds['membership_activity'])) {
        $this->database->insert('ms_fact_membership_activity')
          ->fields([
            'snapshot_id' => $snapshotIds['membership_activity'],
            'joins'       => $joins_count,
            'cancels'     => $cancels_count,
            'net_change'  => $net_change,
          ])->execute();
      }

      // Per-plan dynamic counts.
      $byPlan = [];
      foreach ($active as $r) {
        $code = $r['plan_code'];
        $label = $r['plan_label'] ?: $code;
        if (!isset($byPlan[$code])) {
          $byPlan[$code] = ['plan_code' => $code, 'plan_label' => $label, 'count_members' => 0];
        }
        $byPlan[$code]['count_members']++;
      }

      if (isset($snapshotIds['plan_levels'])) {
        foreach ($byPlan as $plan) {
          $this->database->insert('ms_fact_plan_snapshot')
            ->fields([
              'snapshot_id'    => $snapshotIds['plan_levels'],
              'plan_code'      => $plan['plan_code'],
              'plan_label'     => $plan['plan_label'],
              'count_members'  => $plan['count_members'],
            ])->execute();
        }
      }

      if (isset($snapshotIds['donation_metrics'])) {
        $donationMetrics = $this->calculateDonationMetrics($periodStartObject, $periodEndObject);
        if (!empty($donationMetrics)) {
          $this->database->insert('ms_fact_donation_snapshot')
            ->fields([
              'snapshot_id' => $snapshotIds['donation_metrics'],
              'period_year' => (int) $periodStartObject->format('Y'),
              'period_month' => (int) $periodStartObject->format('m'),
              'donors_count' => $donationMetrics['donors_count'],
              'ytd_unique_donors' => $donationMetrics['ytd_unique_donors'],
              'contributions_count' => $donationMetrics['contributions_count'],
              'recurring_contributions_count' => $donationMetrics['recurring_contributions_count'],
              'onetime_contributions_count' => $donationMetrics['onetime_contributions_count'],
              'recurring_donors_count' => $donationMetrics['recurring_donors_count'],
              'onetime_donors_count' => $donationMetrics['onetime_donors_count'],
              'total_amount' => $donationMetrics['total_amount'],
              'recurring_amount' => $donationMetrics['recurring_amount'],
              'onetime_amount' => $donationMetrics['onetime_amount'],
            ])
            ->execute();
        }
      }

      if (isset($snapshotIds['event_type_metrics'])) {
        $eventTypeMetrics = $this->calculateEventTypeMetrics($periodStartObject, $periodEndObject);
        if (!empty($eventTypeMetrics)) {
          $normalizedType = strtolower((string) $snapshot_type);
          $periodYear = (int) $periodStartObject->format('Y');
          $monthValue = (int) $periodStartObject->format('n');
          $periodMonth = $normalizedType === 'annually' ? 0 : $monthValue;
          $periodQuarter = $normalizedType === 'annually' ? 0 : (int) ceil($monthValue / 3);

          foreach ($eventTypeMetrics as $metric) {
            $this->database->insert('ms_fact_event_type_snapshot')
              ->fields([
                'snapshot_id' => $snapshotIds['event_type_metrics'],
                'period_year' => $periodYear,
                'period_quarter' => $periodQuarter,
                'period_month' => $periodMonth,
                'event_type_id' => $metric['event_type_id'],
                'event_type_label' => $metric['event_type_label'],
                'participant_count' => $metric['participant_count'],
                'total_amount' => $metric['total_amount'],
                'average_ticket' => $metric['average_ticket'],
              ])
              ->execute();
          }
        }
      }

      if (isset($snapshotIds['tool_availability'])) {
        $toolMetrics = $this->calculateToolAvailabilityMetrics($periodStartObject, $periodEndObject);
        if (!empty($toolMetrics)) {
          $this->database->insert('ms_fact_tool_uptime_snapshot')
            ->fields([
              'snapshot_id' => $snapshotIds['tool_availability'],
              'period_year' => $toolMetrics['period_year'],
              'period_month' => $toolMetrics['period_month'],
              'period_day' => $toolMetrics['period_day'],
              'total_tools' => $toolMetrics['total_tools'],
              'available_tools' => $toolMetrics['available_tools'],
              'down_tools' => $toolMetrics['down_tools'],
              'maintenance_tools' => $toolMetrics['maintenance_tools'],
              'unknown_tools' => $toolMetrics['unknown_tools'],
              'availability_percent' => $toolMetrics['availability_percent'],
            ])
            ->execute();
        }
      }

      if (isset($snapshotIds['membership_totals'])) {
        $snapshot_id = $snapshotIds['membership_totals'];
        $kpiContext = [
          'snapshot_id' => $snapshot_id,
          'snapshot_type' => $snapshot_type,
          'snapshot_date' => $snapshotDate,
          'period_start' => $periodStart,
          'period_end' => $periodEnd,
          'members_active' => $members_active,
          'members_paused' => $members_paused,
          'members_lapsed' => $members_lapsed,
          'joins' => $joins_count,
          'cancels' => $cancels_count,
          'net_change' => $net_change,
          'is_test' => $isTest,
          'source' => $source,
        ];
        $kpiValues = $this->collectKpiMetrics($kpiContext);
        if (!empty($kpiValues)) {
          $this->storeKpiMetrics($snapshot_id, $snapshotDate, $kpiValues);
        }
      }

      $this->logger->info("Snapshots stored for {$snapshotDate} (" . implode(', ', array_keys($snapshotIds)) . ")");

      $this->pruneSnapshots();
    } catch (\Exception $e) {
      $this->logger->error('Error taking snapshot: @message', ['@message' => $e->getMessage()]);
    }
  }

  /**
   * Returns snapshot source queries with labels and descriptions.
   */
  public function getSourceQueries(): array {
    $queries = $this->sourceQueries;
    $this->moduleHandler->alter('makerspace_snapshot_source_queries', $queries);
    return $queries;
  }

  /**
   * Returns dataset-to-query mapping metadata.
   */
  public function getDatasetSourceMap(): array {
    $map = $this->datasetSourceMap;
    $this->moduleHandler->alter('makerspace_snapshot_dataset_sources', $map);
    return $map;
  }

  public function importSnapshot($definition, $schedule, $snapshot_date, array $payload) {
    $definitions = $this->buildDefinitions();
    if (!isset($definitions[$definition])) {
      throw new \Exception("Invalid snapshot definition: {$definition}");
    }

    $snapshot_id = $payload['snapshot_id'] ?? NULL;

    if (!$snapshot_id) {
      $snapshot_id = $this->database->insert('ms_snapshot')
        ->fields([
          'definition'    => $definition,
          'snapshot_type' => $schedule,
          'snapshot_date' => $snapshot_date,
          'source'        => 'manual_import',
          'created_at'    => time(),
        ])->execute();
    }
    else {
      $this->database->update('ms_snapshot')
        ->fields([
          'snapshot_type' => $schedule,
          'snapshot_date' => $snapshot_date,
          'source' => 'manual_import',
          'created_at' => time(),
        ])
        ->condition('id', $snapshot_id)
        ->execute();
    }

    if (!$snapshot_id) {
      throw new \Exception("Failed to create or update snapshot for {$snapshot_date}");
    }

    switch ($definition) {
      case 'membership_totals':
        $this->importMembershipSnapshot($snapshot_id, $payload);
        break;
      case 'membership_activity':
        $this->importMembershipActivitySnapshot($snapshot_id, $payload);
        break;
      case 'donation_metrics':
        $this->importDonationMetricsSnapshot($snapshot_id, $payload);
        break;
      case 'event_registrations':
        $this->importEventSnapshot($snapshot_id, $payload);
        break;
      case 'event_type_metrics':
        $this->importEventTypeMetricsSnapshot($snapshot_id, $payload);
        break;
      case 'survey_metrics':
        $this->importSurveyMetricsSnapshot($snapshot_id, $payload);
        break;
      case 'tool_availability':
        $this->importToolAvailabilitySnapshot($snapshot_id, $payload);
        break;
    }

    return $snapshot_id;
  }

  protected function importMembershipSnapshot($snapshot_id, array $payload) {
    $this->database->merge('ms_fact_org_snapshot')
      ->key(['snapshot_id' => $snapshot_id])
      ->fields([
        'members_active' => $payload['totals']['members_active'],
        'members_paused' => $payload['totals']['members_paused'],
        'members_lapsed' => $payload['totals']['members_lapsed'],
        'joins'          => 0,
        'cancels'        => 0,
        'net_change'     => 0,
      ])->execute();

    if (isset($payload['plans'])) {
      foreach ($payload['plans'] as $plan) {
        $this->database->merge('ms_fact_plan_snapshot')
          ->key(['snapshot_id' => $snapshot_id, 'plan_code' => $plan['plan_code']])
          ->fields([
            'plan_label'     => $plan['plan_label'],
            'count_members'  => $plan['count_members'],
          ])->execute();
      }
    }
  }

  protected function importMembershipActivitySnapshot($snapshot_id, array $payload) {
    $this->database->merge('ms_fact_membership_activity')
      ->key(['snapshot_id' => $snapshot_id])
      ->fields([
        'joins'       => $payload['activity']['joins'],
        'cancels'     => $payload['activity']['cancels'],
        'net_change'  => $payload['activity']['net_change'],
      ])->execute();
  }

  protected function importDonationMetricsSnapshot($snapshot_id, array $payload) {
    $metrics = $payload['metrics'] ?? [];
    if (!is_array($metrics) || empty($metrics)) {
      return;
    }

    $fields = [
      'snapshot_id' => $snapshot_id,
      'period_year' => (int) ($metrics['period_year'] ?? date('Y')),
      'period_month' => (int) ($metrics['period_month'] ?? 0),
      'donors_count' => (int) ($metrics['donors_count'] ?? 0),
      'ytd_unique_donors' => (int) ($metrics['ytd_unique_donors'] ?? 0),
      'contributions_count' => (int) ($metrics['contributions_count'] ?? 0),
      'recurring_contributions_count' => (int) ($metrics['recurring_contributions_count'] ?? 0),
      'onetime_contributions_count' => (int) ($metrics['onetime_contributions_count'] ?? 0),
      'recurring_donors_count' => (int) ($metrics['recurring_donors_count'] ?? 0),
      'onetime_donors_count' => (int) ($metrics['onetime_donors_count'] ?? 0),
      'total_amount' => round((float) ($metrics['total_amount'] ?? 0), 2),
      'recurring_amount' => round((float) ($metrics['recurring_amount'] ?? 0), 2),
      'onetime_amount' => round((float) ($metrics['onetime_amount'] ?? 0), 2),
    ];

    $this->database->merge('ms_fact_donation_snapshot')
      ->key(['snapshot_id' => $snapshot_id])
      ->fields($fields)
      ->execute();
  }

  protected function importEventSnapshot($snapshot_id, array $payload) {
    foreach ($payload['events'] as $event) {
      $this->database->merge('ms_fact_event_snapshot')
        ->key(['snapshot_id' => $snapshot_id, 'event_id' => $event['event_id']])
        ->fields([
          'event_title'        => $event['event_title'],
          'event_start_date'   => $event['event_start_date'],
          'registration_count' => $event['registration_count'],
        ])->execute();
    }
  }

  protected function importEventTypeMetricsSnapshot($snapshot_id, array $payload) {
    if (empty($payload['event_types']) || !is_array($payload['event_types'])) {
      return;
    }

    $this->database->delete('ms_fact_event_type_snapshot')
      ->condition('snapshot_id', $snapshot_id)
      ->execute();

    foreach ($payload['event_types'] as $row) {
      $this->database->insert('ms_fact_event_type_snapshot')
        ->fields([
          'snapshot_id' => $snapshot_id,
          'period_year' => (int) ($row['period_year'] ?? 0),
          'period_quarter' => (int) ($row['period_quarter'] ?? 0),
          'period_month' => (int) ($row['period_month'] ?? 0),
          'event_type_id' => isset($row['event_type_id']) && $row['event_type_id'] !== '' ? (int) $row['event_type_id'] : NULL,
          'event_type_label' => (string) ($row['event_type_label'] ?? 'Unknown'),
          'participant_count' => (int) ($row['participant_count'] ?? 0),
          'total_amount' => round((float) ($row['total_amount'] ?? 0), 2),
          'average_ticket' => round((float) ($row['average_ticket'] ?? 0), 2),
        ])
        ->execute();
    }
  }

  protected function importSurveyMetricsSnapshot($snapshot_id, array $payload) {
    $metrics = $payload['metrics'] ?? [];
    if (!is_array($metrics) || empty($metrics)) {
      return;
    }

    $fields = [
      'snapshot_id' => $snapshot_id,
      'period_year' => (int) ($metrics['period_year'] ?? date('Y')),
      'period_month' => (int) ($metrics['period_month'] ?? 0),
      'period_day' => (int) ($metrics['period_day'] ?? 0),
      'timeframe_label' => (string) ($metrics['timeframe_label'] ?? ''),
      'respondents_count' => (int) ($metrics['respondents_count'] ?? 0),
      'likely_recommend' => round((float) ($metrics['likely_recommend'] ?? 0), 2),
      'net_promoter_score' => round((float) ($metrics['net_promoter_score'] ?? 0), 2),
      'satisfaction_rating' => round((float) ($metrics['satisfaction_rating'] ?? 0), 2),
      'equipment_score' => round((float) ($metrics['equipment_score'] ?? 0), 2),
      'learning_resources_score' => round((float) ($metrics['learning_resources_score'] ?? 0), 2),
      'member_events_score' => round((float) ($metrics['member_events_score'] ?? 0), 2),
      'paid_workshops_score' => round((float) ($metrics['paid_workshops_score'] ?? 0), 2),
      'facility_score' => round((float) ($metrics['facility_score'] ?? 0), 2),
      'community_score' => round((float) ($metrics['community_score'] ?? 0), 2),
      'vibe_score' => round((float) ($metrics['vibe_score'] ?? 0), 2),
    ];

    $this->database->merge('ms_fact_survey_snapshot')
      ->key(['snapshot_id' => $snapshot_id])
      ->fields($fields)
      ->execute();
  }

  protected function importToolAvailabilitySnapshot($snapshot_id, array $payload) {
    $metrics = $payload['metrics'] ?? [];
    if (!is_array($metrics) || empty($metrics)) {
      return;
    }

    $fields = [
      'snapshot_id' => $snapshot_id,
      'period_year' => (int) ($metrics['period_year'] ?? date('Y')),
      'period_month' => (int) ($metrics['period_month'] ?? 0),
      'period_day' => (int) ($metrics['period_day'] ?? 0),
      'total_tools' => (int) ($metrics['total_tools'] ?? 0),
      'available_tools' => (int) ($metrics['available_tools'] ?? 0),
      'down_tools' => (int) ($metrics['down_tools'] ?? 0),
      'maintenance_tools' => (int) ($metrics['maintenance_tools'] ?? 0),
      'unknown_tools' => (int) ($metrics['unknown_tools'] ?? 0),
      'availability_percent' => round((float) ($metrics['availability_percent'] ?? 0), 2),
    ];

    $this->database->merge('ms_fact_tool_uptime_snapshot')
      ->key(['snapshot_id' => $snapshot_id])
      ->fields($fields)
      ->execute();
  }

  /**
   * Prunes old snapshots.
   */
  public function pruneSnapshots() {
    $retention_window_months = $this->configFactory->get('makerspace_snapshot.settings')->get('retention_window_months');
    if (empty($retention_window_months)) {
      return;
    }

    $retention_date = (new \DateTime())->modify("-{$retention_window_months} months")->format('Y-m-d');

    $query = $this->database->select('ms_snapshot', 's');
    $query->fields('s', ['id']);
    $query->condition('snapshot_date', $retention_date, '<');
    $results = $query->execute()->fetchAll();

    if (empty($results)) {
      return;
    }

    $snapshot_ids = array_map(function ($row) {
      return $row->id;
    }, $results);

    $this->database->delete('ms_snapshot')
      ->condition('id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_org_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_plan_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_membership_activity')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_donation_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_event_type_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_survey_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_event_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_kpi_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->logger->info('Pruned @count snapshots older than @date.', ['@count' => count($snapshot_ids), '@date' => $retention_date]);
  }

  /**
   * Computes inclusive period bounds for the snapshot cadence.
   */
  protected function resolvePeriodBounds(\DateTimeImmutable $snapshotDate, string $snapshotType): array {
    $normalized = strtolower($snapshotType);
    $start = $snapshotDate->setTime(0, 0, 0);
    switch ($normalized) {
      case 'quarterly':
        $year = (int) $start->format('Y');
        $month = (int) $start->format('n');
        $quarterIndex = (int) floor(($month - 1) / 3);
        $quarterStartMonth = ($quarterIndex * 3) + 1;
        $start = $start->setDate($year, $quarterStartMonth, 1);
        $end = $start->modify('+2 months')->modify('last day of this month')->setTime(23, 59, 59);
        break;

      case 'annually':
        $year = (int) $start->format('Y');
        $start = $start->setDate($year, 1, 1);
        $end = $start->setDate($year, 12, 31)->setTime(23, 59, 59);
        break;

      default:
        $end = $start->modify('last day of this month')->setTime(23, 59, 59);
        break;
    }

    return [
      'start' => $start,
      'end' => $end,
    ];
  }

  /**
   * Aggregates event metrics grouped by event type for the reporting window.
   */
  protected function calculateEventTypeMetrics(\DateTimeImmutable $periodStart, \DateTimeImmutable $periodEnd): array {
    $schema = $this->database->schema();
    if (
      !$schema->tableExists('civicrm_participant') ||
      !$schema->tableExists('civicrm_event') ||
      !$schema->tableExists('civicrm_participant_status_type') ||
      !$schema->tableExists('civicrm_participant_payment') ||
      !$schema->tableExists('civicrm_contribution')
    ) {
      return [];
    }

    $eventTypeGroupId = $this->getOptionGroupId('event_type');
    $contributionStatusGroupId = $this->getOptionGroupId('contribution_status');

    $query = $this->database->select('civicrm_participant', 'p');
    $query->innerJoin('civicrm_event', 'e', 'e.id = p.event_id');
    $query->innerJoin('civicrm_participant_status_type', 'pst', 'pst.id = p.status_id');
    $query->leftJoin('civicrm_participant_payment', 'pp', 'pp.participant_id = p.id');
    $query->leftJoin('civicrm_contribution', 'c', 'c.id = pp.contribution_id');

    if ($eventTypeGroupId) {
      $query->leftJoin('civicrm_option_value', 'ov', 'ov.option_group_id = ' . (int) $eventTypeGroupId . ' AND ov.value = e.event_type_id');
    }
    else {
      $query->leftJoin('civicrm_option_value', 'ov', 'ov.value = e.event_type_id');
    }

    if ($contributionStatusGroupId) {
      $query->leftJoin('civicrm_option_value', 'cs', 'cs.option_group_id = ' . (int) $contributionStatusGroupId . ' AND cs.value = c.contribution_status_id');
    }

    $query->condition('pst.is_counted', 1);
    $query->condition('pst.is_cancelled', 0);
    $query->condition('e.start_date', [$periodStart->format('Y-m-d H:i:s'), $periodEnd->format('Y-m-d H:i:s')], 'BETWEEN');

    $query->addExpression('COALESCE(ov.value, e.event_type_id)', 'event_type_id');
    $query->addExpression("COALESCE(ov.label, CONCAT('Type ', e.event_type_id))", 'event_type_label');
    $query->addExpression('COUNT(DISTINCT p.id)', 'participant_count');

    $revenueArgs = [];
    if ($contributionStatusGroupId) {
      $disallowedNames = ['cancelled', 'refunded', 'pending refund', 'pending_refund', 'chargeback', 'failed'];
      $placeholders = [];
      foreach ($disallowedNames as $index => $statusName) {
        $placeholder = ':disallowed_status_' . $index;
        $placeholders[] = $placeholder;
        $revenueArgs[$placeholder] = strtolower($statusName);
      }
      if ($placeholders) {
        $placeholderString = implode(', ', $placeholders);
        $revenueExpression = "SUM(CASE WHEN c.id IS NULL OR COALESCE(c.is_test, 0) = 1 THEN 0 WHEN LOWER(cs.name) IN ($placeholderString) THEN 0 ELSE COALESCE(c.total_amount, 0) END)";
      }
      else {
        $revenueExpression = 'SUM(CASE WHEN c.id IS NULL OR COALESCE(c.is_test, 0) = 1 THEN 0 ELSE COALESCE(c.total_amount, 0) END)';
      }
    }
    else {
      $disallowedIds = [3, 7, 8, 9];
      $placeholders = [];
      foreach ($disallowedIds as $index => $statusId) {
        $placeholder = ':disallowed_status_id_' . $index;
        $placeholders[] = $placeholder;
        $revenueArgs[$placeholder] = $statusId;
      }
      if ($placeholders) {
        $placeholderString = implode(', ', $placeholders);
        $revenueExpression = "SUM(CASE WHEN c.id IS NULL OR COALESCE(c.is_test, 0) = 1 THEN 0 WHEN c.contribution_status_id IN ($placeholderString) THEN 0 ELSE COALESCE(c.total_amount, 0) END)";
      }
      else {
        $revenueExpression = 'SUM(CASE WHEN c.id IS NULL OR COALESCE(c.is_test, 0) = 1 THEN 0 ELSE COALESCE(c.total_amount, 0) END)';
      }
    }

    $query->addExpression($revenueExpression, 'total_revenue', $revenueArgs);

    $query->groupBy('event_type_id');
    $query->groupBy('event_type_label');
    $query->orderBy('event_type_label', 'ASC');

    $records = $query->execute();

    $metrics = [];
    foreach ($records as $record) {
      $participants = (int) $record->participant_count;
      $totalAmount = round((float) $record->total_revenue, 2);
      $label = trim((string) $record->event_type_label) !== '' ? (string) $record->event_type_label : 'Unknown';
      $eventTypeId = isset($record->event_type_id) && $record->event_type_id !== '' ? (int) $record->event_type_id : NULL;
      $average = $participants > 0 ? round($totalAmount / $participants, 2) : 0.0;

      if ($participants === 0 && $totalAmount === 0.0) {
        continue;
      }

      $metrics[] = [
        'event_type_id' => $eventTypeId,
        'event_type_label' => $label,
        'participant_count' => $participants,
        'total_amount' => $totalAmount,
        'average_ticket' => $average,
      ];
    }

    return $metrics;
  }

  /**
   * Placeholder tool availability metrics until asset_status aggregation lands.
   */
  protected function calculateToolAvailabilityMetrics(\DateTimeImmutable $periodStart, \DateTimeImmutable $periodEnd): array {
    $periodYear = (int) $periodStart->format('Y');
    $periodMonth = (int) $periodStart->format('n');
    $periodDay = (int) $periodStart->format('j');

    $metrics = [
      'period_year' => $periodYear,
      'period_month' => $periodMonth,
      'period_day' => $periodDay,
      'total_tools' => 0,
      'available_tools' => 0,
      'down_tools' => 0,
      'maintenance_tools' => 0,
      'unknown_tools' => 0,
      'availability_percent' => 0.0,
    ];

    if (!$this->moduleHandler->moduleExists('asset_status')) {
      return $metrics;
    }

    // @todo Leverage asset_status once uptime calculation APIs are available.
    return $metrics;
  }

  /**
   * Returns the option group ID for a given CiviCRM option group name.
   */
  protected function getOptionGroupId(string $groupName): ?int {
    static $cache = [];
    $key = strtolower($groupName);

    if (array_key_exists($key, $cache)) {
      return $cache[$key];
    }

    $schema = $this->database->schema();
    if (!$schema->tableExists('civicrm_option_group')) {
      $cache[$key] = NULL;
      return NULL;
    }

    $query = $this->database->select('civicrm_option_group', 'og')
      ->fields('og', ['id'])
      ->condition('og.name', $groupName);
    $id = $query->execute()->fetchField();
    $cache[$key] = $id ? (int) $id : NULL;

    return $cache[$key];
  }

  /**
   * Computes donation metrics for the given reporting window.
   */
  protected function calculateDonationMetrics(\DateTimeImmutable $periodStart, \DateTimeImmutable $periodEnd): array {
    $schema = $this->database->schema();
    if (!$schema->tableExists('civicrm_contribution')) {
      return [
        'donors_count' => 0,
        'ytd_unique_donors' => 0,
        'contributions_count' => 0,
        'recurring_contributions_count' => 0,
        'onetime_contributions_count' => 0,
        'recurring_donors_count' => 0,
        'onetime_donors_count' => 0,
        'total_amount' => 0.0,
        'recurring_amount' => 0.0,
        'onetime_amount' => 0.0,
      ];
    }

    $start = $periodStart->format('Y-m-d H:i:s');
    $end = $periodEnd->format('Y-m-d H:i:s');

    $baseQuery = $this->database->select('civicrm_contribution', 'c');
    $baseQuery->addExpression('COUNT(DISTINCT c.id)', 'contribution_count');
    $baseQuery->addExpression('COUNT(DISTINCT IF(c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0, c.id, NULL))', 'recurring_contribution_count');
    $baseQuery->addExpression('COUNT(DISTINCT c.contact_id)', 'donor_count');
    $baseQuery->addExpression('COUNT(DISTINCT IF(c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0, c.contact_id, NULL))', 'recurring_donor_count');
    $baseQuery->addExpression('COUNT(DISTINCT IF(c.contribution_recur_id IS NULL OR c.contribution_recur_id = 0, c.contact_id, NULL))', 'onetime_donor_count');
    $baseQuery->addExpression('SUM(COALESCE(c.total_amount, 0))', 'total_amount');
    $baseQuery->addExpression('SUM(IF(c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0, COALESCE(c.total_amount, 0), 0))', 'recurring_amount');
    $baseQuery->condition('c.receive_date', [$start, $end], 'BETWEEN');
    $baseQuery->condition('c.contribution_status_id', 1);
    $baseQuery->condition('c.is_test', 0);
    $baseQuery->condition('COALESCE(c.total_amount, 0)', 0, '>');
    $baseQuery->isNotNull('c.contact_id');

    $result = $baseQuery->execute()->fetchObject();

    if (!$result) {
      return [
        'donors_count' => 0,
        'ytd_unique_donors' => 0,
        'contributions_count' => 0,
        'recurring_contributions_count' => 0,
        'onetime_contributions_count' => 0,
        'recurring_donors_count' => 0,
        'onetime_donors_count' => 0,
        'total_amount' => 0.0,
        'recurring_amount' => 0.0,
        'onetime_amount' => 0.0,
      ];
    }

    $contributionsCount = (int) ($result->contribution_count ?? 0);
    $recurringContributionsCount = (int) ($result->recurring_contribution_count ?? 0);
    $donorsCount = (int) ($result->donor_count ?? 0);
    $recurringDonorsCount = (int) ($result->recurring_donor_count ?? 0);
    $oneTimeDonorsCount = (int) ($result->onetime_donor_count ?? 0);
    $totalAmount = (float) ($result->total_amount ?? 0.0);
    $recurringAmount = (float) ($result->recurring_amount ?? 0.0);
    $oneTimeAmount = max(0.0, $totalAmount - $recurringAmount);
    $oneTimeContributionsCount = max(0, $contributionsCount - $recurringContributionsCount);

    $yearStart = $periodEnd->setDate((int) $periodEnd->format('Y'), 1, 1)->setTime(0, 0, 0);
    $ytdQuery = $this->database->select('civicrm_contribution', 'c');
    $ytdQuery->addExpression('COUNT(DISTINCT c.contact_id)', 'donor_count');
    $ytdQuery->condition('c.receive_date', [$yearStart->format('Y-m-d H:i:s'), $end], 'BETWEEN');
    $ytdQuery->condition('c.contribution_status_id', 1);
    $ytdQuery->condition('c.is_test', 0);
    $ytdQuery->condition('COALESCE(c.total_amount, 0)', 0, '>');
    $ytdQuery->isNotNull('c.contact_id');
    $ytdUniqueDonors = (int) $ytdQuery->execute()->fetchField();

    return [
      'donors_count' => $donorsCount,
      'ytd_unique_donors' => $ytdUniqueDonors,
      'contributions_count' => $contributionsCount,
      'recurring_contributions_count' => $recurringContributionsCount,
      'onetime_contributions_count' => $oneTimeContributionsCount,
      'recurring_donors_count' => $recurringDonorsCount,
      'onetime_donors_count' => $oneTimeDonorsCount,
      'total_amount' => $totalAmount,
      'recurring_amount' => $recurringAmount,
      'onetime_amount' => $oneTimeAmount,
    ];
  }

  /**
   * Collects KPI metrics from subscriber modules.
   *
   * @param array $context
   *   Contextual data describing the snapshot in progress.
   *
   * @return array
   *   An array keyed by KPI ID containing arrays with at least a 'value' key.
   */
  protected function collectKpiMetrics(array $context): array {
    $results = $this->moduleHandler->invokeAll('makerspace_snapshot_collect_kpi', [$context]);
    if (empty($results)) {
      return [];
    }

    $metrics = [];
    foreach ($results as $result) {
      if (!is_array($result)) {
        continue;
      }
      foreach ($result as $kpiId => $info) {
        if (!is_string($kpiId) || $kpiId === '') {
          continue;
        }
        $metrics[$kpiId] = $this->normalizeKpiMetric($info);
      }
    }

    return array_filter($metrics, static function ($row) {
      return $row !== NULL && isset($row['value']);
    });
  }

  /**
   * Persists collected KPI metrics for the snapshot.
   */
  protected function storeKpiMetrics(int $snapshot_id, string $snapshotDate, array $metrics): void {
    $snapshotDateObj = \DateTimeImmutable::createFromFormat('Y-m-d', $snapshotDate) ?: new \DateTimeImmutable();
    $defaultYear = (int) $snapshotDateObj->format('Y');
    $defaultMonth = (int) $snapshotDateObj->format('m');

    $this->database->delete('ms_fact_kpi_snapshot')
      ->condition('snapshot_id', $snapshot_id)
      ->execute();

    foreach ($metrics as $kpiId => $info) {
      if (!isset($info['value'])) {
        continue;
      }
      $value = $info['value'];
      $periodYear = isset($info['period_year']) ? (int) $info['period_year'] : $defaultYear;
      $periodMonth = isset($info['period_month']) ? (int) $info['period_month'] : $defaultMonth;
      $meta = $info['meta'] ?? [];

      $fields = [
        'snapshot_id' => $snapshot_id,
        'kpi_id' => $kpiId,
        'metric_value' => is_numeric($value) ? (float) $value : $value,
        'period_year' => $periodYear ?: NULL,
        'period_month' => $periodMonth ?: NULL,
        'meta' => !empty($meta) ? $meta : NULL,
      ];

      $this->database->insert('ms_fact_kpi_snapshot')
        ->fields($fields)
        ->execute();
    }

    $tags = ['makerspace_snapshot:kpi'];
    foreach (array_keys($metrics) as $kpiId) {
      $tags[] = 'makerspace_snapshot:kpi:' . $kpiId;
    }
    Cache::invalidateTags($tags);
  }

  /**
   * Normalizes a KPI metric row.
   *
   * @param mixed $info
   *   The raw info returned by a hook subscriber.
   *
   * @return array|null
   *   Normalized metric info or NULL if it cannot be parsed.
   */
  protected function normalizeKpiMetric($info): ?array {
    if (is_scalar($info)) {
      return ['value' => $info];
    }

    if (!is_array($info) || !isset($info['value'])) {
      return NULL;
    }

    $row = [
      'value' => $info['value'],
    ];
    if (isset($info['period_year'])) {
      $row['period_year'] = (int) $info['period_year'];
    }
    if (isset($info['period_month'])) {
      $row['period_month'] = (int) $info['period_month'];
    }
    if (isset($info['meta']) && is_array($info['meta'])) {
      $row['meta'] = $info['meta'];
    }

    return $row;
  }

  /**
   * Deletes a snapshot and its associated data.
   *
   * @param int $snapshot_id
   *   The ID of the snapshot to delete.
   */
  public function deleteSnapshot($snapshot_id) {
    if (!$snapshot_id) {
      throw new \InvalidArgumentException('Snapshot ID is required.');
    }

    $transaction = $this->database->startTransaction();
    try {
      $this->database->delete('ms_snapshot')
        ->condition('id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_org_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_plan_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_membership_activity')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_donation_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_event_type_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_survey_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_event_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_kpi_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();

      $this->logger->info('Deleted snapshot with ID @id.', ['@id' => $snapshot_id]);
    }
    catch (\Exception $e) {
      $transaction->rollBack();
      $this->logger->error('Error deleting snapshot with ID @id: @message', ['@id' => $snapshot_id, '@message' => $e->getMessage()]);
      throw $e;
    }
  }
}
