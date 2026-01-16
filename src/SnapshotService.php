<?php

namespace Drupal\makerspace_snapshot;

use Psr\Log\LoggerInterface;
use Drupal\Core\Cache\Cache;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\Extension\ModuleHandlerInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;

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
   * Entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected EntityTypeManagerInterface $entityTypeManager;

  /**
   * Cached membership type term info.
   *
   * @var array|null
   */
  protected ?array $membershipTypeTerms = NULL;

  /**
   * Cached plan level definitions keyed by plan code.
   *
   * @var array|null
   */
  protected ?array $planLevelDefinitions = NULL;

  /**
   * Cached event type definitions keyed by event type ID.
   *
   * @var array|null
   */
  protected ?array $eventTypeDefinitions = NULL;

  /**
   * Cached donation range definitions keyed by range ID.
   *
   * @var array|null
   */
  protected ?array $donationRangeDefinitions = NULL;

  /**
   * Cached indicator for donation snapshot first-time donor column.
   *
   * @var bool|null
   */
  protected ?bool $donationSnapshotHasFirstTimeColumn = NULL;

  /**
   * Cached indicator for donation range snapshot table availability.
   *
   * @var bool|null
   */
  protected ?bool $donationRangeSnapshotAvailable = NULL;

  /**
   * Fallback donation range definitions when no config is stored.
   */
  protected const DONATION_RANGE_DEFAULTS = [
    ['id' => 'under_100', 'label' => 'Under $100', 'min' => 0, 'max' => 99.99],
    ['id' => '100_249', 'label' => '$100 - $249', 'min' => 100, 'max' => 249.99],
    ['id' => '250_499', 'label' => '$250 - $499', 'min' => 250, 'max' => 499.99],
    ['id' => '500_999', 'label' => '$500 - $999', 'min' => 500, 'max' => 999.99],
    ['id' => '1000_2499', 'label' => '$1,000 - $2,499', 'min' => 1000, 'max' => 2499.99],
    ['id' => '2500_4999', 'label' => '$2,500 - $4,999', 'min' => 2500, 'max' => 4999.99],
    ['id' => '5000_plus', 'label' => '$5,000+', 'min' => 5000, 'max' => NULL],
  ];

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
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'UNASSIGNED'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'UNASSIGNED'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_code,
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'Unassigned'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'Unassigned'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_label
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id AND r.roles_target_id = 'member'
LEFT JOIN user__field_chargebee_payment_pause cb_pause ON cb_pause.entity_id = u.uid AND cb_pause.deleted = 0
LEFT JOIN user__field_manual_pause manual_pause ON manual_pause.entity_id = u.uid AND manual_pause.deleted = 0
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
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
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'UNASSIGNED'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'UNASSIGNED'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_code,
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'Unassigned (Paused)'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'Unassigned (Paused)'
         ELSE CONCAT(plan.field_user_chargebee_plan_value, ' (Paused)')
       END AS plan_label
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id AND r.roles_target_id = 'member'
LEFT JOIN user__field_chargebee_payment_pause cb_pause ON cb_pause.entity_id = u.uid AND cb_pause.deleted = 0
LEFT JOIN user__field_manual_pause manual_pause ON manual_pause.entity_id = u.uid AND manual_pause.deleted = 0
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
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
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'UNASSIGNED'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'UNASSIGNED'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_code,
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'Unassigned'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'Unassigned'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_label,
       FROM_UNIXTIME(u.created, '%Y-%m-%d') AS occurred_at
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
WHERE r.roles_target_id = 'member'
  AND u.created BETWEEN UNIX_TIMESTAMP(:start) AND UNIX_TIMESTAMP(:end)
SQL,
    ],
    'sql_cancels' => [
      'label' => 'Cancels in period',
      'description' => 'Records membership cancellations based on the profile end date falling between :start and :end.',
      'sql' => <<<SQL
SELECT u.uid AS member_id,
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'UNASSIGNED'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'UNASSIGNED'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_code,
       CASE
         WHEN NULLIF(plan.field_user_chargebee_plan_value, '') IS NULL THEN 'Unassigned'
         WHEN LOCATE('@', plan.field_user_chargebee_plan_value) > 0 THEN 'Unassigned'
         ELSE plan.field_user_chargebee_plan_value
       END AS plan_label,
       ed.field_member_end_date_value AS occurred_at
FROM users_field_data u
INNER JOIN profile p ON p.uid = u.uid AND p.type = 'main'
INNER JOIN profile__field_member_end_date ed ON ed.entity_id = p.profile_id AND ed.deleted = 0
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
WHERE ed.field_member_end_date_value BETWEEN :start AND :end
SQL,
    ],
    'sql_event_type_metrics' => [
      'label' => 'Event type metrics',
      'description' => 'Aggregates counted participants and revenue by event type for events whose start_date falls between :start and :end.',
      'sql' => <<<SQL
SELECT
  CASE WHEN e.event_type_id IS NULL OR e.event_type_id = 0 THEN NULL ELSE e.event_type_id END AS event_type_id,
  COALESCE(ov.label, CONCAT('Type ', e.event_type_id)) AS event_type_label,
  COUNT(DISTINCT e.id) AS events_count,
  COUNT(DISTINCT p.id) AS participant_count,
  SUM(
    CASE
      WHEN c.id IS NULL THEN 0
      WHEN COALESCE(c.is_test, 0) = 1 THEN 0
      WHEN c.contribution_status_id IN (3, 7, 8, 9) THEN 0
      ELSE COALESCE(c.total_amount, 0)
    END
  ) AS total_amount,
  CASE
    WHEN COUNT(DISTINCT p.id) = 0 THEN 0
    ELSE ROUND(
      SUM(
        CASE
          WHEN c.id IS NULL THEN 0
          WHEN COALESCE(c.is_test, 0) = 1 THEN 0
          WHEN c.contribution_status_id IN (3, 7, 8, 9) THEN 0
          ELSE COALESCE(c.total_amount, 0)
        END
      ) / COUNT(DISTINCT p.id),
      2
    )
  END AS average_ticket
FROM civicrm_participant p
INNER JOIN civicrm_event e ON e.id = p.event_id
INNER JOIN civicrm_participant_status_type pst ON pst.id = p.status_id
LEFT JOIN civicrm_participant_payment pp ON pp.participant_id = p.id
LEFT JOIN civicrm_contribution c ON c.id = pp.contribution_id
LEFT JOIN civicrm_option_value ov ON ov.value = e.event_type_id
LEFT JOIN civicrm_option_group og ON og.id = ov.option_group_id
WHERE pst.is_counted = 1
  AND pst.is_cancelled = 0
  AND COALESCE(e.is_active, 0) = 1
  AND COALESCE(e.is_template, 0) = 0
  AND (og.name = 'event_type' OR og.name IS NULL)
  AND e.start_date BETWEEN :start AND :end
GROUP BY event_type_id, event_type_label
ORDER BY event_type_label ASC
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
    'plan_levels' => [
      'label' => 'Plan Levels',
      'description' => 'Counts active members by plan code using the active member snapshot.',
      'queries' => ['sql_active'],
    ],
    'membership_types' => [
      'label' => 'Membership Types',
      'description' => 'Breaks down current members by membership type taxonomy terms.',
      'queries' => ['sql_active', 'sql_paused'],
    ],
    'membership_type_joins' => [
      'label' => 'Membership Type Joins',
      'description' => 'Counts membership joins grouped by membership type for the reporting window.',
      'queries' => ['sql_joins'],
    ],
    'membership_type_cancels' => [
      'label' => 'Membership Type Cancels',
      'description' => 'Counts membership cancellations grouped by membership type for the reporting window.',
      'queries' => ['sql_cancels'],
    ],
    'donation_metrics' => [
      'label' => 'Donation Metrics',
      'description' => 'Aggregates donor and contribution metrics for the reporting period.',
      'queries' => [],
    ],
    'donation_range_metrics' => [
      'label' => 'Donation Range Metrics',
      'description' => 'Summarizes donors, gifts, and dollars by annual gift range.',
      'queries' => [],
    ],
    'event_type_metrics' => [
      'label' => 'Event Type Metrics',
      'description' => 'Summarizes counted event registrations and revenue by event type.',
      'queries' => ['sql_event_type_metrics'],
    ],
    'event_type_counts' => [
      'label' => 'Events Held by Type',
      'description' => 'Derived dataset summarizing the number of events per event type.',
      'queries' => [],
    ],
    'event_type_registrations' => [
      'label' => 'Event Registrations by Type',
      'description' => 'Derived dataset summarizing counted registrations per event type.',
      'queries' => [],
    ],
    'event_type_revenue' => [
      'label' => 'Event Revenue by Type',
      'description' => 'Derived dataset summarizing revenue per event type.',
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
      'headers' => [
        'snapshot_date',
        'members_active',
        'members_paused',
        'members_lapsed',
        'members_total',
        'joins',
        'cancels',
        'net_change',
      ],
      'dataset_type' => 'membership_totals',
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
        'first_time_donors_count',
        'total_amount',
        'recurring_amount',
        'onetime_amount',
      ],
      'dataset_type' => 'donation_metrics',
      'acquisition' => 'automated',
      'data_source' => 'CiviCRM SQL',
    ],
    'donation_range_metrics' => [
      'label' => 'Donation Range Metrics',
      'schedules' => ['monthly'],
      'headers' => [
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
      'dataset_type' => 'donation_range_metrics',
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
      'headers' => ['snapshot_date'],
      'dataset_type' => 'plan_levels',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'membership_types' => [
      'label' => 'Membership Types',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'members_total'],
      'dataset_type' => 'membership_types',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'membership_type_joins' => [
      'label' => 'Membership Type Joins',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'joins_total'],
      'dataset_type' => 'membership_type_joins',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'membership_type_cancels' => [
      'label' => 'Membership Type Cancels',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date', 'cancels_total'],
      'dataset_type' => 'membership_type_cancels',
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
        'events_count',
        'participant_count',
        'total_amount',
        'average_ticket',
      ],
      'dataset_type' => 'event_type_metrics',
      'acquisition' => 'automated',
      'data_source' => 'CiviCRM SQL',
    ],
    'event_type_counts' => [
      'label' => 'Events Held by Type',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date'],
      'dataset_type' => 'event_type_counts',
      'acquisition' => 'derived',
      'data_source' => 'Derived from Event Type Metrics',
    ],
    'event_type_registrations' => [
      'label' => 'Event Registrations by Type',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date'],
      'dataset_type' => 'event_type_registrations',
      'acquisition' => 'derived',
      'data_source' => 'Derived from Event Type Metrics',
    ],
    'event_type_revenue' => [
      'label' => 'Event Revenue by Type',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => ['snapshot_date'],
      'dataset_type' => 'event_type_revenue',
      'acquisition' => 'derived',
      'data_source' => 'Derived from Event Type Metrics',
    ],
    'survey_metrics' => [
      'label' => 'Member Survey Metrics',
      'schedules' => ['annually'],
      'headers' => [
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
   * Cache tags keyed by dataset definition.
   *
   * @var array
   */
  protected array $cacheTagsByDefinition = [
    'membership_totals' => ['makerspace_snapshot:org'],
    'plan_levels' => ['makerspace_snapshot:plan'],
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
  public function __construct(Connection $database, LoggerInterface $logger, ConfigFactoryInterface $config_factory, ?ModuleHandlerInterface $module_handler = NULL, ?EntityTypeManagerInterface $entity_type_manager = NULL) {
    $this->database = $database;
    $this->logger = $logger;
    $this->configFactory = $config_factory;
    $this->moduleHandler = $module_handler ?? \Drupal::moduleHandler();
    $this->entityTypeManager = $entity_type_manager ?? \Drupal::entityTypeManager();
  }

  public function buildDefinitions() {
    $definitions = $this->datasetDefinitions;
    if (isset($definitions['plan_levels'])) {
      $plans = $this->getPlanLevelDefinitions();
      $headers = ['snapshot_date'];
      foreach ($plans as $code => $info) {
        $headers[] = $code;
      }
      $definitions['plan_levels']['headers'] = $headers;
    }
    $hasMembershipTypeDatasets = array_intersect(
      ['membership_types', 'membership_type_joins', 'membership_type_cancels'],
      array_keys($definitions)
    );
    if (!empty($hasMembershipTypeDatasets)) {
      $terms = $this->getMembershipTypeTerms();
      if (isset($definitions['membership_types'])) {
        $headers = ['snapshot_date', 'members_total'];
        foreach ($terms as $tid => $info) {
          $headers[] = 'membership_type_' . $tid;
        }
        $definitions['membership_types']['headers'] = $headers;
      }
      if (isset($definitions['membership_type_joins'])) {
        $headers = ['snapshot_date', 'joins_total'];
        foreach ($terms as $tid => $info) {
          $headers[] = 'membership_type_' . $tid;
        }
        $definitions['membership_type_joins']['headers'] = $headers;
      }
      if (isset($definitions['membership_type_cancels'])) {
        $headers = ['snapshot_date', 'cancels_total'];
        foreach ($terms as $tid => $info) {
          $headers[] = 'membership_type_' . $tid;
        }
        $definitions['membership_type_cancels']['headers'] = $headers;
      }
    }
    $eventTypeColumns = $this->getEventTypeColumns();
    $eventTypeHeader = ['snapshot_date'];
    foreach (array_keys($eventTypeColumns) as $column) {
      $eventTypeHeader[] = $column;
    }
    foreach (['event_type_counts', 'event_type_registrations', 'event_type_revenue'] as $definition) {
      if (isset($definitions[$definition])) {
        $definitions[$definition]['headers'] = $eventTypeHeader;
      }
    }
    return $definitions;
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

      $eventTypeDerived = ['event_type_counts', 'event_type_registrations', 'event_type_revenue'];
      $needsEventMetrics = array_intersect($selectedDefinitions, array_merge(['event_type_metrics'], $eventTypeDerived));
      if (!empty($needsEventMetrics)) {
        $selectedDefinitions = array_values(array_unique(array_merge($selectedDefinitions, ['event_type_metrics'], $eventTypeDerived)));
      }

      if (in_array('donation_range_metrics', $selectedDefinitions, TRUE) && !in_array('donation_metrics', $selectedDefinitions, TRUE)) {
        $selectedDefinitions[] = 'donation_metrics';
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
      $members_total = $members_active + $members_paused;
      $joins_count    = count($joins);
      $cancels_count  = count($cancels);
      $net_change     = $joins_count - $cancels_count;

      $membershipTypeData = [];
      if (in_array('membership_types', $selectedDefinitions, TRUE)) {
        $membershipTypeData = $this->calculateMembershipTypeBreakdown($active, $paused);
      }

      $membershipTypeJoinsData = [];
      if (in_array('membership_type_joins', $selectedDefinitions, TRUE)) {
        $membershipTypeJoinsData = $this->calculateMembershipTypeBreakdown($joins, []);
      }

      $membershipTypeCancelsData = [];
      if (in_array('membership_type_cancels', $selectedDefinitions, TRUE)) {
        $membershipTypeCancelsData = $this->calculateMembershipTypeBreakdown($cancels, []);
      }

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
            'members_total'  => $members_total,
            'members_active' => $members_active,
            'members_paused' => $members_paused,
            'members_lapsed' => $members_lapsed,
            'joins'          => $joins_count,
            'cancels'        => $cancels_count,
            'net_change'     => $net_change,
          ])->execute();
        $this->invalidateDatasetCache('membership_totals');
      }

      if (isset($snapshotIds['membership_types'])) {
        $this->persistMembershipTypeCounts(
          (int) $snapshotIds['membership_types'],
          $membershipTypeData['counts'] ?? [],
          $membershipTypeData['terms'] ?? [],
          $members_total
        );
      }

      if (isset($snapshotIds['membership_type_joins'])) {
        $joinCounts = $membershipTypeJoinsData['counts'] ?? [];
        $joinTerms = $membershipTypeJoinsData['terms'] ?? [];
        $this->persistMembershipTypeCounts(
          (int) $snapshotIds['membership_type_joins'],
          $joinCounts,
          $joinTerms,
          array_sum($joinCounts)
        );
      }

      if (isset($snapshotIds['membership_type_cancels'])) {
        $cancelCounts = $membershipTypeCancelsData['counts'] ?? [];
        $cancelTerms = $membershipTypeCancelsData['terms'] ?? [];
        $this->persistMembershipTypeCounts(
          (int) $snapshotIds['membership_type_cancels'],
          $cancelCounts,
          $cancelTerms,
          array_sum($cancelCounts)
        );
      }

      // Per-plan dynamic counts.
      $planDefinitions = $this->getPlanLevelDefinitions();
      $byPlan = [];
      foreach ($active as $r) {
        $code = $this->normalizePlanCode($r['plan_code'] ?? '');
        $label = $planDefinitions[$code]['label'] ?? $this->formatPlanLabel($r['plan_label'] ?? $code);
        if (!isset($byPlan[$code])) {
          $byPlan[$code] = ['plan_code' => $code, 'plan_label' => $label, 'count_members' => 0];
        }
        $byPlan[$code]['count_members']++;
      }

      if (isset($snapshotIds['plan_levels'])) {
        foreach ($byPlan as $plan) {
          $this->ensurePlanLevelDefinition($plan['plan_code'], $plan['plan_label']);
          $this->database->insert('ms_fact_plan_snapshot')
            ->fields([
              'snapshot_id'    => $snapshotIds['plan_levels'],
              'plan_code'      => $plan['plan_code'],
              'plan_label'     => $plan['plan_label'],
              'count_members'  => $plan['count_members'],
            ])->execute();
        }
        $this->invalidateDatasetCache('plan_levels');
      }

      $needsDonationMetrics = isset($snapshotIds['donation_metrics']) || isset($snapshotIds['donation_range_metrics']);
      $donationMetrics = NULL;
      if ($needsDonationMetrics) {
        $includeRangeBreakdown = isset($snapshotIds['donation_range_metrics']);
        $donationMetrics = $this->calculateDonationMetrics($periodStartObject, $periodEndObject, $includeRangeBreakdown);
      }

      if ($donationMetrics && isset($snapshotIds['donation_metrics'])) {
        $fields = [
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
        ];
        if ($this->donationSnapshotHasFirstTimeColumn()) {
          $fields['first_time_donors_count'] = $donationMetrics['first_time_donors_count'];
        }
        $this->database->insert('ms_fact_donation_snapshot')
          ->fields($fields)
          ->execute();
      }

      if ($donationMetrics && isset($snapshotIds['donation_range_metrics'])) {
        $this->persistDonationRangeSnapshot(
          (int) $snapshotIds['donation_range_metrics'],
          $donationMetrics['range_breakdown'] ?? [],
          (int) $periodStartObject->format('Y'),
          (int) $periodStartObject->format('m'),
          TRUE
        );
      }

      $eventTypeSnapshotDefinitions = array_intersect(
        ['event_type_metrics', 'event_type_counts', 'event_type_registrations', 'event_type_revenue'],
        array_keys($snapshotIds)
      );
      if (!empty($eventTypeSnapshotDefinitions)) {
        $eventTypeMetrics = $this->calculateEventTypeMetrics($periodStartObject, $periodEndObject);
        if (!empty($eventTypeMetrics)) {
          $normalizedType = strtolower((string) $snapshot_type);
          $periodYear = (int) $periodStartObject->format('Y');
          $monthValue = (int) $periodStartObject->format('n');
          $periodMonth = $normalizedType === 'annually' ? 0 : $monthValue;
          $periodQuarter = $normalizedType === 'annually' ? 0 : (int) ceil($monthValue / 3);

          foreach ($eventTypeSnapshotDefinitions as $definition) {
            $this->persistEventTypeSnapshot(
              (int) $snapshotIds[$definition],
              $eventTypeMetrics,
              $periodYear,
              $periodQuarter,
              $periodMonth
            );
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
          'members_total' => $members_total,
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

  /**
   * Builds a label row aligned to the provided header.
   */
  public function getDatasetLabelRow(string $definition, array $header): array {
    $labels = [];
    switch ($definition) {
      case 'plan_levels':
        $plans = $this->getPlanLevelDefinitions();
        foreach ($header as $column) {
          if ($column === 'snapshot_date') {
            $labels[] = 'Snapshot Date';
          }
          else {
            $labels[] = $plans[$column]['label'] ?? $this->formatPlanLabel($column);
          }
        }
        return $labels;

      case 'membership_types':
        $terms = $this->getMembershipTypeTerms();
        foreach ($header as $column) {
          if ($column === 'snapshot_date') {
            $labels[] = 'Snapshot Date';
          }
          elseif ($column === 'members_total') {
            $labels[] = 'Members Total';
          }
          elseif (strpos($column, 'membership_type_') === 0) {
            $tid = (int) substr($column, strlen('membership_type_'));
            $labels[] = $terms[$tid]['label'] ?? ('Membership Type ' . $tid);
          }
          else {
            $labels[] = $this->defaultColumnLabel($column);
          }
        }
        return $labels;
      case 'membership_type_joins':
      case 'membership_type_cancels':
        $terms = $this->getMembershipTypeTerms();
        foreach ($header as $column) {
          if ($column === 'snapshot_date') {
            $labels[] = 'Snapshot Date';
          }
          elseif ($column === 'joins_total') {
            $labels[] = 'Total Joins';
          }
          elseif ($column === 'cancels_total') {
            $labels[] = 'Total Cancels';
          }
          elseif (strpos($column, 'membership_type_') === 0) {
            $tid = (int) substr($column, strlen('membership_type_'));
            $labels[] = $terms[$tid]['label'] ?? ('Membership Type ' . $tid);
          }
          else {
            $labels[] = $this->defaultColumnLabel($column);
          }
        }
        return $labels;

      case 'event_type_counts':
      case 'event_type_registrations':
      case 'event_type_revenue':
        $types = $this->getEventTypeColumns();
        foreach ($header as $column) {
          if ($column === 'snapshot_date') {
            $labels[] = 'Snapshot Date';
          }
          else {
            $labels[] = $types[$column] ?? $this->defaultColumnLabel($column);
          }
        }
        return $labels;

      default:
        foreach ($header as $column) {
          $labels[] = $this->defaultColumnLabel($column);
        }
        return $labels;
    }
  }

  /**
   * Returns snapshot export options keyed by "type|date".
   */
  public function getSnapshotExportOptions(): array {
    $options = [];
    $query = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_type', 'snapshot_date'])
      ->distinct()
      ->orderBy('snapshot_date', 'DESC')
      ->orderBy('snapshot_type', 'ASC');
    $results = $query->execute();
    foreach ($results as $row) {
      $type = (string) ($row->snapshot_type ?? '');
      $date = (string) $row->snapshot_date;
      if ($date === '') {
        continue;
      }
      $key = $type . '|' . $date;
      $options[$key] = $this->formatTypeLabel($type ?: 'monthly') . ' – ' . $this->formatSnapshotDateLabel($date);
    }
    return $options;
  }

  /**
   * Gathers snapshot data for export across supported definitions.
   */
  public function getSnapshotExportData(string $snapshot_type, string $snapshot_date): array {
    try {
      $normalizedDate = (new \DateTimeImmutable($snapshot_date))->format('Y-m-01');
    }
    catch (\Exception $e) {
      $normalizedDate = $snapshot_date;
    }

    $snapshotRows = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id', 'definition'])
      ->condition('snapshot_type', $snapshot_type)
      ->condition('snapshot_date', $normalizedDate)
      ->execute()
      ->fetchAllAssoc('definition');

    if (empty($snapshotRows)) {
      return [];
    }

    $definitions = $this->buildDefinitions();
    $data = [];

    $eventTypeRowsCache = [];
    $loadEventTypeRows = function (?int $snapshotId) use (&$eventTypeRowsCache) {
      if (!$snapshotId) {
        return [];
      }
      if (!isset($eventTypeRowsCache[$snapshotId])) {
        $eventTypeRowsCache[$snapshotId] = $this->database->select('ms_fact_event_type_snapshot', 'e')
          ->fields('e', [
            'event_type_id',
            'event_type_label',
            'events_count',
            'participant_count',
            'total_amount',
            'average_ticket',
            'period_year',
            'period_quarter',
            'period_month',
          ])
          ->condition('snapshot_id', $snapshotId)
          ->orderBy('event_type_label')
          ->execute()
          ->fetchAll(\PDO::FETCH_ASSOC);
      }
      return $eventTypeRowsCache[$snapshotId];
    };

    // Membership totals.
    if (isset($definitions['membership_totals'])) {
      $header = $definitions['membership_totals']['headers'];
      $rows = [];
      $totalsId = $snapshotRows['membership_totals']->id ?? NULL;
      if ($totalsId) {
        $record = $this->database->select('ms_fact_org_snapshot', 'o')
          ->fields('o')
          ->condition('snapshot_id', $totalsId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $values = array_fill_keys($header, '');
          $values['snapshot_date'] = $normalizedDate;
          foreach (['members_total', 'members_active', 'members_paused', 'members_lapsed', 'joins', 'cancels', 'net_change'] as $key) {
            if (isset($record[$key]) && array_key_exists($key, $values)) {
              $values[$key] = (int) $record[$key];
            }
          }
          $rows[] = array_values($values);
        }
      }
      $data['membership_totals'] = [
        'filename' => 'membership_totals.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('membership_totals', $header),
        'rows' => $rows,
      ];
    }

    // Membership activity.
    // Plan levels.
    if (isset($definitions['plan_levels'])) {
      $header = $definitions['plan_levels']['headers'];
      $rows = [];

      $planId = $snapshotRows['plan_levels']->id ?? NULL;
      if ($planId) {
        $result = $this->database->select('ms_fact_plan_snapshot', 'p')
          ->fields('p', ['plan_code', 'plan_label', 'count_members'])
          ->condition('snapshot_id', $planId)
          ->orderBy('plan_code')
          ->execute();
        $values = array_fill_keys($header, '');
        $values['snapshot_date'] = $normalizedDate;
        $hasData = FALSE;
        foreach ($result as $record) {
          $code = $this->normalizePlanCode($record->plan_code ?? '');
          if (!array_key_exists($code, $values)) {
            $values[$code] = '';
          }
          $values[$code] = (int) $record->count_members;
          $hasData = TRUE;
        }
        if ($hasData) {
          $rows[] = array_map(function ($column) use ($values, $header) {
            return $values[$column] ?? '';
          }, $header);
        }
      }

      $data['plan_levels'] = [
        'filename' => 'plan_levels.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('plan_levels', $header),
        'rows' => $rows,
      ];
    }

    // Event registrations.
    if (isset($definitions['event_registrations'])) {
      $header = $definitions['event_registrations']['headers'];
      $rows = [];
      $eventId = $snapshotRows['event_registrations']->id ?? NULL;
      if ($eventId) {
        $result = $this->database->select('ms_fact_event_snapshot', 'e')
          ->fields('e', ['event_id', 'event_title', 'event_start_date', 'registration_count'])
          ->condition('snapshot_id', $eventId)
          ->orderBy('event_start_date')
          ->execute();
        foreach ($result as $record) {
          $rows[] = [
            $normalizedDate,
            $record->event_id,
            $record->event_title,
            $record->event_start_date,
            (int) $record->registration_count,
          ];
        }
      }
      $data['event_registrations'] = [
        'filename' => 'event_registrations.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('event_registrations', $header),
        'rows' => $rows,
      ];
    }

    // Membership types.
    if (isset($definitions['membership_types'])) {
      $header = $definitions['membership_types']['headers'];
      $rows = $this->buildMembershipTypeDatasetRow(
        $snapshotRows['membership_types']->id ?? NULL,
        $header,
        $normalizedDate,
        'members_total'
      );
      $data['membership_types'] = [
        'filename' => 'membership_types.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('membership_types', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['membership_type_joins'])) {
      $header = $definitions['membership_type_joins']['headers'];
      $rows = $this->buildMembershipTypeDatasetRow(
        $snapshotRows['membership_type_joins']->id ?? NULL,
        $header,
        $normalizedDate,
        'joins_total'
      );
      $data['membership_type_joins'] = [
        'filename' => 'membership_type_joins.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('membership_type_joins', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['membership_type_cancels'])) {
      $header = $definitions['membership_type_cancels']['headers'];
      $rows = $this->buildMembershipTypeDatasetRow(
        $snapshotRows['membership_type_cancels']->id ?? NULL,
        $header,
        $normalizedDate,
        'cancels_total'
      );
      $data['membership_type_cancels'] = [
        'filename' => 'membership_type_cancels.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('membership_type_cancels', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['donation_metrics'])) {
      $header = $definitions['donation_metrics']['headers'];
      $rows = [];
      $donationId = $snapshotRows['donation_metrics']->id ?? NULL;
      if ($donationId) {
        $record = $this->database->select('ms_fact_donation_snapshot', 'd')
          ->fields('d')
          ->condition('snapshot_id', $donationId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $values = array_fill_keys($header, '');
          $values['snapshot_date'] = $normalizedDate;
          foreach ($header as $column) {
            if ($column === 'snapshot_date' || !isset($record[$column])) {
              continue;
            }
            if (in_array($column, ['total_amount', 'recurring_amount', 'onetime_amount'], TRUE)) {
              $values[$column] = round((float) $record[$column], 2);
            }
            else {
              $values[$column] = (int) $record[$column];
            }
          }
          $rows[] = array_values($values);
        }
      }
      $data['donation_metrics'] = [
        'filename' => 'donation_metrics.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('donation_metrics', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['donation_range_metrics'])) {
      if (!$this->donationRangeSnapshotTableExists()) {
        $data['donation_range_metrics'] = [
          'filename' => 'donation_range_metrics.csv',
          'header' => $definitions['donation_range_metrics']['headers'],
          'label_row' => $this->getDatasetLabelRow('donation_range_metrics', $definitions['donation_range_metrics']['headers']),
          'rows' => [],
        ];
      }
      else {
        $header = $definitions['donation_range_metrics']['headers'];
        $rows = [];
        $rangeSnapshotId = $snapshotRows['donation_range_metrics']->id ?? NULL;
        if ($rangeSnapshotId) {
          $result = $this->database->select('ms_fact_donation_range_snapshot', 'r')
          ->fields('r', [
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
          ])
          ->condition('snapshot_id', $rangeSnapshotId)
          ->orderBy('min_amount')
          ->execute();
        foreach ($result as $record) {
          $rows[] = [
            $normalizedDate,
            (int) $record->period_year,
            (int) $record->period_month,
            (int) $record->is_year_to_date,
            (string) $record->range_key,
            (string) $record->range_label,
            round((float) $record->min_amount, 2),
            $record->max_amount === NULL ? '' : round((float) $record->max_amount, 2),
            (int) $record->donors_count,
            (int) $record->contributions_count,
            round((float) $record->total_amount, 2),
          ];
        }
      }

        $data['donation_range_metrics'] = [
          'filename' => 'donation_range_metrics.csv',
          'header' => $header,
          'label_row' => $this->getDatasetLabelRow('donation_range_metrics', $header),
          'rows' => $rows,
        ];
      }
    }

    if (isset($definitions['event_type_metrics'])) {
      $header = $definitions['event_type_metrics']['headers'];
      $rows = [];
      $snapshotId = $snapshotRows['event_type_metrics']->id ?? NULL;
      foreach ($loadEventTypeRows($snapshotId) as $record) {
        $row = [];
        foreach ($header as $column) {
          switch ($column) {
            case 'snapshot_date':
              $row[] = $normalizedDate;
              break;

            case 'event_type_label':
              $row[] = (string) ($record['event_type_label'] ?? '');
              break;

            case 'event_type_id':
              $row[] = isset($record['event_type_id']) && $record['event_type_id'] !== NULL
                ? (int) $record['event_type_id']
                : '';
              break;

            case 'total_amount':
            case 'average_ticket':
              $row[] = round((float) ($record[$column] ?? 0), 2);
              break;

            default:
              $row[] = (int) ($record[$column] ?? 0);
              break;
          }
        }
        $rows[] = $row;
      }
      $data['event_type_metrics'] = [
        'filename' => 'event_type_metrics.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('event_type_metrics', $header),
        'rows' => $rows,
      ];
    }

    $eventTypeColumns = $this->getEventTypeColumns();
    $eventTypeHeader = ['snapshot_date'];
    foreach (array_keys($eventTypeColumns) as $column) {
      $eventTypeHeader[] = $column;
    }

    $buildEventTypePivotRow = function (?int $snapshotId, string $valueKey, bool $isCurrency = FALSE) use ($loadEventTypeRows, $eventTypeColumns, $eventTypeHeader, $normalizedDate) {
      if (!$snapshotId) {
        return [];
      }
      $values = array_fill_keys($eventTypeHeader, '');
      $values['snapshot_date'] = $normalizedDate;
      $hasData = FALSE;
      foreach ($loadEventTypeRows($snapshotId) as $record) {
        $column = $this->mapEventTypeColumn($record['event_type_id'] ?? NULL);
        if (!array_key_exists($column, $values)) {
          $values[$column] = '';
        }
        if (isset($record[$valueKey])) {
          $hasData = TRUE;
          $values[$column] = $isCurrency
            ? round((float) $record[$valueKey], 2)
            : (int) $record[$valueKey];
        }
      }
      if (!$hasData) {
        return [];
      }
      $row = [];
      foreach ($eventTypeHeader as $column) {
        $row[] = $values[$column];
      }
      return [$row];
    };

    if (isset($definitions['event_type_counts'])) {
      $snapshotId = $snapshotRows['event_type_counts']->id ?? NULL;
      $rows = $buildEventTypePivotRow($snapshotId, 'events_count', FALSE);
      $data['event_type_counts'] = [
        'filename' => 'event_type_counts.csv',
        'header' => $eventTypeHeader,
        'label_row' => $this->getDatasetLabelRow('event_type_counts', $eventTypeHeader),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['event_type_registrations'])) {
      $snapshotId = $snapshotRows['event_type_registrations']->id ?? NULL;
      $rows = $buildEventTypePivotRow($snapshotId, 'participant_count', FALSE);
      $data['event_type_registrations'] = [
        'filename' => 'event_type_registrations.csv',
        'header' => $eventTypeHeader,
        'label_row' => $this->getDatasetLabelRow('event_type_registrations', $eventTypeHeader),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['event_type_revenue'])) {
      $snapshotId = $snapshotRows['event_type_revenue']->id ?? NULL;
      $rows = $buildEventTypePivotRow($snapshotId, 'total_amount', TRUE);
      $data['event_type_revenue'] = [
        'filename' => 'event_type_revenue.csv',
        'header' => $eventTypeHeader,
        'label_row' => $this->getDatasetLabelRow('event_type_revenue', $eventTypeHeader),
        'rows' => $rows,
      ];
    }


    if (isset($definitions['survey_metrics'])) {
      $header = $definitions['survey_metrics']['headers'];
      $rows = [];
      $surveyId = $snapshotRows['survey_metrics']->id ?? NULL;
      if ($surveyId) {
        $record = $this->database->select('ms_fact_survey_snapshot', 's')
          ->fields('s')
          ->condition('snapshot_id', $surveyId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $values = array_fill_keys($header, '');
          $values['snapshot_date'] = $normalizedDate;
          $floatColumns = [
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
          ];
          foreach ($header as $column) {
            if ($column === 'snapshot_date' || !isset($record[$column])) {
              continue;
            }
            if ($column === 'timeframe_label') {
              $values[$column] = (string) $record[$column];
            }
            elseif (in_array($column, $floatColumns, TRUE)) {
              $values[$column] = round((float) $record[$column], 2);
            }
            else {
              $values[$column] = (int) $record[$column];
            }
          }
          $rows[] = array_values($values);
        }
      }
      $data['survey_metrics'] = [
        'filename' => 'survey_metrics.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('survey_metrics', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['tool_availability'])) {
      $header = $definitions['tool_availability']['headers'];
      $rows = [];
      $toolId = $snapshotRows['tool_availability']->id ?? NULL;
      if ($toolId) {
        $record = $this->database->select('ms_fact_tool_uptime_snapshot', 't')
          ->fields('t')
          ->condition('snapshot_id', $toolId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $values = array_fill_keys($header, '');
          $values['snapshot_date'] = $normalizedDate;
          foreach ($header as $column) {
            if ($column === 'snapshot_date' || !isset($record[$column])) {
              continue;
            }
            if ($column === 'availability_percent') {
              $values[$column] = round((float) $record[$column], 2);
            }
            else {
              $values[$column] = (int) $record[$column];
            }
          }
          $rows[] = array_values($values);
        }
      }
      $data['tool_availability'] = [
        'filename' => 'tool_availability.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('tool_availability', $header),
        'rows' => $rows,
      ];
    }

    return $data;
  }

  /**
   * Returns plan level metadata keyed by plan code.
   */
  public function getPlanLevelDefinitions(): array {
    if ($this->planLevelDefinitions !== NULL) {
      return $this->planLevelDefinitions;
    }

    $config = $this->configFactory->getEditable('makerspace_snapshot.plan_levels');
    $stored = $config->get('plan_levels') ?? [];
    $plans = [];
    foreach ($stored as $item) {
      $code = $this->normalizePlanCode($item['code'] ?? '');
      if ($code === '') {
        continue;
      }
      $plans[$code] = [
        'label' => $item['label'] ?? $code,
        'group' => $item['group'] ?? '',
      ];
    }

    $dirty = FALSE;
    $discovered = $this->discoverPlanCodes();
    foreach ($discovered as $code => $label) {
      $normalizedCode = $this->normalizePlanCode($code);
      if ($normalizedCode === '') {
        continue;
      }
      $resolvedLabel = $label !== '' ? $label : $normalizedCode;
      if (!isset($plans[$normalizedCode])) {
        $plans[$normalizedCode] = [
          'label' => $resolvedLabel,
          'group' => '',
        ];
        $dirty = TRUE;
      }
      elseif (($plans[$normalizedCode]['label'] ?? '') === '' && $resolvedLabel !== '') {
        $plans[$normalizedCode]['label'] = $resolvedLabel;
        $dirty = TRUE;
      }
    }

    if ($dirty) {
      $this->savePlanLevelDefinitions($plans);
      return $this->planLevelDefinitions ?? [];
    }

    $this->planLevelDefinitions = $plans;
    return $plans;
  }

  /**
   * Registers a plan definition if it does not already exist.
   */
  public function registerPlanLevelDefinition(string $code, string $label = ''): void {
    $this->ensurePlanLevelDefinition($code, $label);
  }

  /**
   * Returns membership type term metadata keyed by term ID.
   */
  protected function getMembershipTypeTerms(): array {
    if ($this->membershipTypeTerms !== NULL) {
      return $this->membershipTypeTerms;
    }

    $config = $this->configFactory->getEditable('makerspace_snapshot.membership_types');
    $stored = $config->get('columns') ?? [];
    $columns = [];
    foreach ($stored as $item) {
      $tid = isset($item['tid']) ? (int) $item['tid'] : 0;
      if ($tid <= 0) {
        continue;
      }
      $columns[$tid] = [
        'label' => $item['label'] ?? ('Membership Type ' . $tid),
      ];
    }

    $dirty = FALSE;

    $targetBundles = [];
    try {
      $field = $this->entityTypeManager
        ->getStorage('field_config')
        ->load('profile.main.field_membership_type');
      if ($field) {
        $settings = $field->getSetting('handler_settings') ?? [];
        if (!empty($settings['target_bundles']) && is_array($settings['target_bundles'])) {
          $targetBundles = array_keys($settings['target_bundles']);
        }
      }
    }
    catch (\Exception $e) {
      $this->logger->error('Unable to load field configuration for membership types: @message', ['@message' => $e->getMessage()]);
    }

    try {
      $term_storage = $this->entityTypeManager->getStorage('taxonomy_term');
      $query = $term_storage->getQuery()->accessCheck(FALSE);
      if (!empty($targetBundles)) {
        $query->condition('vid', $targetBundles, 'IN');
      }
      $query->condition('status', 1);
      $query->sort('tid');
      $term_ids = $query->execute();
      if (!empty($term_ids)) {
        $term_entities = $term_storage->loadMultiple($term_ids);
        foreach ($term_entities as $term) {
          $tid = (int) $term->id();
          $label = $term->label();
          if (!isset($columns[$tid])) {
            $columns[$tid] = ['label' => $label];
            $dirty = TRUE;
          }
          elseif ($columns[$tid]['label'] !== $label) {
            $columns[$tid]['label'] = $label;
            $dirty = TRUE;
          }
        }
      }
    }
    catch (\Exception $e) {
      $this->logger->error('Unable to load membership type taxonomy terms: @message', ['@message' => $e->getMessage()]);
    }

    ksort($columns);

    if ($dirty) {
      $this->saveMembershipTypeTerms($columns);
    }

    $this->membershipTypeTerms = $columns;
    return $columns;
  }

  /**
   * Calculates membership type counts for the supplied member rows.
   */
  protected function calculateMembershipTypeBreakdown(array $activeRows, array $pausedRows): array {
    $terms = $this->getMembershipTypeTerms();
    $counts = [];
    foreach (array_keys($terms) as $tid) {
      $counts[$tid] = 0;
    }

    $member_ids = [];
    foreach ([$activeRows, $pausedRows] as $rows) {
      foreach ($rows as $row) {
        $member_id = isset($row['member_id']) ? (int) $row['member_id'] : 0;
        if ($member_id) {
          $member_ids[$member_id] = TRUE;
        }
      }
    }

    if (empty($member_ids) || empty($terms)) {
      return [
        'counts' => $counts,
        'terms' => $terms,
      ];
    }

    $member_ids = array_keys($member_ids);

    $configUpdated = FALSE;

    try {
      $query = $this->database->select('profile', 'p')
        ->condition('p.type', 'main')
        ->condition('p.status', 1)
        ->condition('p.uid', $member_ids, 'IN');
      $query->join('profile__field_membership_type', 'mt', 'mt.entity_id = p.profile_id');
      $query->fields('mt', ['field_membership_type_target_id']);
      $result = $query->execute();

      foreach ($result as $record) {
        $tid = (int) $record->field_membership_type_target_id;
        if (!isset($counts[$tid])) {
          $counts[$tid] = 0;
          try {
            $term = $this->entityTypeManager->getStorage('taxonomy_term')->load($tid);
            if ($term) {
              $terms[$tid] = ['label' => $term->label()];
              $configUpdated = TRUE;
            }
            elseif (!isset($terms[$tid])) {
              $terms[$tid] = ['label' => 'Membership Type ' . $tid];
              $configUpdated = TRUE;
            }
          }
          catch (\Exception $e) {
            if (!isset($terms[$tid])) {
              $terms[$tid] = ['label' => 'Membership Type ' . $tid];
              $configUpdated = TRUE;
            }
            $this->logger->error('Unable to resolve membership type term @tid: @message', ['@tid' => $tid, '@message' => $e->getMessage()]);
          }
        }
        $counts[$tid]++;
      }
    }
    catch (\Exception $e) {
      $this->logger->error('Unable to calculate membership type counts: @message', ['@message' => $e->getMessage()]);
    }

    if ($configUpdated) {
      ksort($terms);
      $this->saveMembershipTypeTerms($terms);
    }

    $this->membershipTypeTerms = $terms;
    return [
      'counts' => $counts,
      'terms' => $terms,
    ];
  }

  /**
   * Persists membership type counts into the snapshot fact table.
   */
  protected function persistMembershipTypeCounts(int $snapshotId, array $counts, array $terms, int $membersTotal): void {
    if (empty($terms)) {
      $terms = $this->getMembershipTypeTerms();
    }
    if (empty($terms)) {
      $terms = [0 => ['label' => 'Membership Type 0']];
    }

    $this->database->delete('ms_fact_membership_type_snapshot')
      ->condition('snapshot_id', $snapshotId)
      ->execute();

    foreach ($terms as $tid => $info) {
      $label = $info['label'] ?? ('Membership Type ' . $tid);
      $count = (int) ($counts[$tid] ?? 0);
      $this->database->insert('ms_fact_membership_type_snapshot')
        ->fields([
          'snapshot_id' => $snapshotId,
          'term_id' => $tid,
          'term_label' => $label,
          'members_total' => $membersTotal,
          'member_count' => $count,
        ])->execute();
    }
  }

  /**
   * Persists donation range aggregates for a snapshot.
   */
  protected function persistDonationRangeSnapshot(int $snapshotId, array $ranges, int $periodYear, int $periodMonth, bool $isYearToDate): void {
    if (!$this->donationRangeSnapshotTableExists()) {
      return;
    }

    $this->database->delete('ms_fact_donation_range_snapshot')
      ->condition('snapshot_id', $snapshotId)
      ->execute();

    $definitions = $this->getDonationRangeDefinitions();
    $ordered = [];
    foreach ($definitions as $rangeId => $info) {
      $ordered[$rangeId] = [
        'range_key' => $rangeId,
        'range_label' => $info['label'],
        'min_amount' => (float) $info['min'],
        'max_amount' => $info['max'],
        'donors_count' => 0,
        'contributions_count' => 0,
        'total_amount' => 0.0,
      ];
    }

    foreach ($ranges as $range) {
      $key = isset($range['range_key']) ? (string) $range['range_key'] : '';
      if ($key === '') {
        continue;
      }
      if (!isset($ordered[$key])) {
        $ordered[$key] = [
          'range_key' => $key,
          'range_label' => (string) ($range['range_label'] ?? $key),
          'min_amount' => (float) ($range['min_amount'] ?? 0),
          'max_amount' => array_key_exists('max_amount', $range) ? ($range['max_amount'] === NULL ? NULL : (float) $range['max_amount']) : NULL,
          'donors_count' => 0,
          'contributions_count' => 0,
          'total_amount' => 0.0,
        ];
      }
      $ordered[$key]['range_label'] = (string) ($range['range_label'] ?? $ordered[$key]['range_label']);
      if (isset($range['min_amount'])) {
        $ordered[$key]['min_amount'] = (float) $range['min_amount'];
      }
      if (array_key_exists('max_amount', $range)) {
        $ordered[$key]['max_amount'] = $range['max_amount'] === NULL ? NULL : (float) $range['max_amount'];
      }
      $ordered[$key]['donors_count'] = (int) ($range['donors_count'] ?? 0);
      $ordered[$key]['contributions_count'] = (int) ($range['contributions_count'] ?? 0);
      $ordered[$key]['total_amount'] = round((float) ($range['total_amount'] ?? 0), 2);
    }

    foreach ($ordered as $row) {
      $this->database->insert('ms_fact_donation_range_snapshot')
        ->fields([
          'snapshot_id' => $snapshotId,
          'period_year' => $periodYear,
          'period_month' => $periodMonth,
          'is_year_to_date' => $isYearToDate ? 1 : 0,
          'range_key' => $row['range_key'],
          'range_label' => $row['range_label'],
          'min_amount' => $row['min_amount'],
          'max_amount' => $row['max_amount'],
          'donors_count' => $row['donors_count'],
          'contributions_count' => $row['contributions_count'],
          'total_amount' => $row['total_amount'],
        ])
        ->execute();
    }
  }

  /**
   * Builds membership type dataset rows for export.
   */
  protected function buildMembershipTypeDatasetRow(?int $snapshotId, array $header, string $snapshotDate, string $totalColumn): array {
    if (!$snapshotId) {
      return [];
    }
    $result = $this->database->select('ms_fact_membership_type_snapshot', 'm')
      ->fields('m', ['term_id', 'term_label', 'members_total', 'member_count'])
      ->condition('snapshot_id', $snapshotId)
      ->execute()
      ->fetchAll();
    if (empty($result)) {
      return [];
    }

    $counts = [];
    $total = NULL;
    foreach ($result as $record) {
      $tid = (int) $record->term_id;
      $column = 'membership_type_' . $tid;
      $counts[$column] = (int) $record->member_count;
      if ($total === NULL && isset($record->members_total)) {
        $total = (int) $record->members_total;
      }
    }
    if ($total === NULL) {
      $total = array_sum($counts);
    }

    $row = [];
    foreach ($header as $column) {
      if ($column === 'snapshot_date') {
        $row[] = $snapshotDate;
      }
      elseif ($column === $totalColumn) {
        $row[] = $total;
      }
      elseif (isset($counts[$column])) {
        $row[] = $counts[$column];
      }
      elseif (strpos($column, 'membership_type_') === 0) {
        $row[] = '';
      }
      else {
        $row[] = '';
      }
    }

    return [$row];
  }

  /**
   * Persists membership type term mapping to configuration.
   */
  protected function saveMembershipTypeTerms(array $terms): void {
    ksort($terms);
    $config = $this->configFactory->getEditable('makerspace_snapshot.membership_types');
    $columns = [];
    foreach ($terms as $tid => $info) {
      $columns[] = [
        'tid' => (int) $tid,
        'label' => $info['label'] ?? ('Membership Type ' . $tid),
      ];
    }
    $config->set('columns', $columns)->save();
    $this->membershipTypeTerms = $terms;
  }

  /**
   * Persists plan level definitions.
   */
  protected function savePlanLevelDefinitions(array $plans): void {
    if (empty($plans)) {
      $this->configFactory->getEditable('makerspace_snapshot.plan_levels')
        ->set('plan_levels', [])
        ->save();
      $this->planLevelDefinitions = [];
      return;
    }

    uksort($plans, 'strnatcasecmp');

    $export = [];
    foreach ($plans as $code => $info) {
      $export[] = [
        'code' => $code,
        'label' => $info['label'] ?? $code,
        'group' => $info['group'] ?? '',
      ];
    }

    $this->configFactory->getEditable('makerspace_snapshot.plan_levels')
      ->set('plan_levels', array_values($export))
      ->save();

    $this->planLevelDefinitions = $plans;
  }

  /**
   * Returns donation range definitions keyed by range ID.
   */
  protected function getDonationRangeDefinitions(): array {
    if ($this->donationRangeDefinitions !== NULL) {
      return $this->donationRangeDefinitions;
    }

    $config = $this->configFactory->getEditable('makerspace_snapshot.donation_ranges');
    $stored = $config->get('ranges') ?? [];
    $ranges = $this->normalizeDonationRangeDefinitions($stored);

    if (empty($ranges)) {
      $ranges = $this->normalizeDonationRangeDefinitions(self::DONATION_RANGE_DEFAULTS);
    }

    $this->donationRangeDefinitions = $ranges;
    return $ranges;
  }

  /**
   * Normalizes donation range entries.
   */
  protected function normalizeDonationRangeDefinitions(array $items): array {
    $ranges = [];
    foreach ($items as $item) {
      $id = isset($item['id']) ? strtolower(trim((string) $item['id'])) : '';
      if ($id === '') {
        continue;
      }
      $ranges[$id] = [
        'label' => (string) ($item['label'] ?? strtoupper($id)),
        'min' => isset($item['min']) ? (float) $item['min'] : 0.0,
        'max' => array_key_exists('max', $item) && $item['max'] !== NULL ? (float) $item['max'] : NULL,
      ];
    }
    return $ranges;
  }

  /**
   * Ensures a plan definition exists for the supplied code.
   */
  protected function ensurePlanLevelDefinition(string $code, string $label = ''): void {
    $normalized = $this->normalizePlanCode($code);
    if ($normalized === '') {
      return;
    }
    $plans = $this->getPlanLevelDefinitions();
    if (!isset($plans[$normalized])) {
      $plans[$normalized] = [
        'label' => $label !== '' ? $label : $this->formatPlanLabel($normalized),
        'group' => '',
      ];
      $this->savePlanLevelDefinitions($plans);
    }
    elseif ($label !== '' && $plans[$normalized]['label'] !== $label) {
      $plans[$normalized]['label'] = $label;
      $this->savePlanLevelDefinitions($plans);
    }
  }

  /**
   * Discovers plan codes from user accounts and historical snapshots.
   */
  protected function discoverPlanCodes(): array {
    $codes = [];
    $schema = $this->database->schema();

    if ($schema->tableExists('user__field_user_chargebee_plan')) {
      $query = $this->database->select('user__field_user_chargebee_plan', 'plan')
        ->fields('plan', ['field_user_chargebee_plan_value'])
        ->condition('plan.deleted', 0)
        ->distinct();
      foreach ($query->execute() as $record) {
        $code = $this->normalizePlanCode($record->field_user_chargebee_plan_value ?? '');
        if ($code === '') {
          continue;
        }
        $codes[$code] = $this->formatPlanLabel($record->field_user_chargebee_plan_value ?? '');
      }
    }

    if ($schema->tableExists('ms_fact_plan_snapshot')) {
      $query = $this->database->select('ms_fact_plan_snapshot', 'p')
        ->fields('p', ['plan_code', 'plan_label'])
        ->distinct();
      foreach ($query->execute() as $record) {
        $code = $this->normalizePlanCode($record->plan_code ?? '');
        if ($code === '') {
          continue;
        }
        if (!isset($codes[$code]) || ($codes[$code] === $code && !empty($record->plan_label))) {
          $codes[$code] = $this->formatPlanLabel($record->plan_label ?? $code);
        }
      }
    }

    if (!isset($codes['UNASSIGNED'])) {
      $codes['UNASSIGNED'] = 'Unassigned';
    }

    return $codes;
  }

  /**
   * Normalizes plan codes, defaulting to UNASSIGNED when empty.
   */
  protected function normalizePlanCode($code): string {
    $value = trim((string) $code);
    if ($value === '' || strpos($value, '@') !== FALSE) {
      return 'UNASSIGNED';
    }
    return $value;
  }

  /**
   * Formats a plan label from a raw value.
   */
  protected function formatPlanLabel($value): string {
    $value = trim((string) $value);
    if ($value === '' || strpos($value, '@') !== FALSE) {
      return 'Unassigned';
    }
    return $value;
  }

  /**
   * Returns known event type definitions keyed by ID.
   */
  protected function getEventTypeDefinitions(): array {
    if ($this->eventTypeDefinitions !== NULL) {
      return $this->eventTypeDefinitions;
    }

    $config = $this->configFactory->getEditable('makerspace_snapshot.event_types');
    $stored = $config->get('types') ?? [];
    $definitions = [];
    foreach ($stored as $item) {
      $id = isset($item['id']) ? (int) $item['id'] : 0;
      $definitions[$id] = $item['label'] ?? ('Event Type ' . $id);
    }

    $dirty = FALSE;
    $schema = $this->database->schema();
    $eventTypeGroupId = $this->getOptionGroupId('event_type');
    if ($eventTypeGroupId && $schema->tableExists('civicrm_option_value')) {
      $result = $this->database->select('civicrm_option_value', 'ov')
        ->fields('ov', ['value', 'label'])
        ->condition('ov.option_group_id', $eventTypeGroupId)
        ->condition('ov.is_active', 1)
        ->orderBy('ov.weight', 'ASC')
        ->orderBy('ov.label', 'ASC')
        ->execute();
      foreach ($result as $record) {
        $value = isset($record->value) ? (int) $record->value : 0;
        $label = trim((string) $record->label) ?: ('Event Type ' . $value);
        if (!isset($definitions[$value]) || $definitions[$value] !== $label) {
          $definitions[$value] = $label;
          $dirty = TRUE;
        }
      }
    }

    if (empty($definitions) && $schema->tableExists('ms_fact_event_type_snapshot')) {
      $result = $this->database->select('ms_fact_event_type_snapshot', 'e')
        ->fields('e', ['event_type_id', 'event_type_label'])
        ->distinct()
        ->execute();
      foreach ($result as $record) {
        $value = isset($record->event_type_id) && $record->event_type_id !== NULL ? (int) $record->event_type_id : 0;
        $label = trim((string) $record->event_type_label) ?: ('Event Type ' . $value);
        if (!isset($definitions[$value])) {
          $definitions[$value] = $label;
          $dirty = TRUE;
        }
      }
    }

    if (!isset($definitions[0])) {
      $definitions[0] = 'Unknown Event Type';
      $dirty = TRUE;
    }

    if ($dirty) {
      $this->saveEventTypeDefinitions($definitions);
    }
    else {
      $this->eventTypeDefinitions = $definitions;
    }

    return $definitions;
  }

  /**
   * Persists event type definitions.
   */
  protected function saveEventTypeDefinitions(array $definitions): void {
    ksort($definitions);
    $config = $this->configFactory->getEditable('makerspace_snapshot.event_types');
    $types = [];
    foreach ($definitions as $id => $label) {
      $types[] = [
        'id' => (int) $id,
        'label' => (string) $label,
      ];
    }
    $config->set('types', $types)->save();
    $this->eventTypeDefinitions = $definitions;
  }

  /**
   * Registers an event type definition if new.
   */
  protected function registerEventTypeDefinition(?int $id, string $label): void {
    $value = $id ?? 0;
    $definitions = $this->getEventTypeDefinitions();
    $label = trim($label) ?: ('Event Type ' . $value);
    if (!isset($definitions[$value]) || $definitions[$value] !== $label) {
      $definitions[$value] = $label;
      $this->saveEventTypeDefinitions($definitions);
    }
  }

  /**
   * Returns event type column metadata keyed by column machine name.
   */
  protected function getEventTypeColumns(): array {
    $definitions = $this->getEventTypeDefinitions();
    $columns = [];
    foreach ($definitions as $id => $label) {
      $columns[$this->mapEventTypeColumn($id)] = $label;
    }
    return $columns;
  }

  /**
   * Converts an event type ID to a column machine name.
   */
  protected function mapEventTypeColumn(?int $eventTypeId): string {
    $value = $eventTypeId ?? 0;
    return 'event_type_' . $value;
  }

  /**
   * Produces a human-friendly label for a dataset column.
   */
  protected function defaultColumnLabel(string $column): string {
    if ($column === 'snapshot_date') {
      return 'Snapshot Date';
    }
    $label = str_replace('_', ' ', $column);
    $label = preg_replace('/\s+/', ' ', $label);
    $label = trim($label);
    return $label === '' ? '' : ucwords($label);
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
        $this->importMembershipSnapshot((int) $snapshot_id, $payload);
        break;
      case 'plan_levels':
        $this->importPlanLevelsSnapshot($snapshot_id, $payload);
        break;
      case 'donation_metrics':
        $this->importDonationMetricsSnapshot($snapshot_id, $payload);
        break;
      case 'donation_range_metrics':
        $this->importDonationRangeSnapshot($snapshot_id, $payload);
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
      case 'membership_types':
      case 'membership_type_joins':
      case 'membership_type_cancels':
        $this->importMembershipTypesSnapshot($snapshot_id, $payload);
        break;
    }

    return $snapshot_id;
  }

  protected function importMembershipSnapshot($snapshot_id, array $payload) {
    $members_active = (int) ($payload['totals']['members_active'] ?? 0);
    $members_paused = (int) ($payload['totals']['members_paused'] ?? 0);
    $members_lapsed = (int) ($payload['totals']['members_lapsed'] ?? 0);
    $members_total = (int) ($payload['totals']['members_total'] ?? ($members_active + $members_paused));
    $joins = (int) ($payload['totals']['joins'] ?? 0);
    $cancels = (int) ($payload['totals']['cancels'] ?? 0);
    $net_change = array_key_exists('net_change', $payload['totals'] ?? [])
      ? (int) $payload['totals']['net_change']
      : ($joins - $cancels);

    $this->database->delete('ms_fact_org_snapshot')
      ->condition('snapshot_id', (int) $snapshot_id)
      ->execute();

    $this->database->insert('ms_fact_org_snapshot')
      ->fields([
        'snapshot_id'    => (int) $snapshot_id,
        'members_total'  => $members_total,
        'members_active' => $members_active,
        'members_paused' => $members_paused,
        'members_lapsed' => $members_lapsed,
        'joins'          => $joins,
        'cancels'        => $cancels,
        'net_change'     => $net_change,
      ])->execute();
    $this->invalidateDatasetCache('membership_totals');

    if (isset($payload['plans'])) {
      $this->importPlanLevelsSnapshot($snapshot_id, ['plans' => $payload['plans']]);
    }
  }

  /**
   * Imports plan level membership counts.
   *
   * @param int $snapshot_id
   *   Snapshot identifier.
   * @param array $payload
   *   Array containing a 'plans' key with rows to persist.
   */
  protected function importPlanLevelsSnapshot($snapshot_id, array $payload) {
    $plans = $payload['plans'] ?? [];
    if (empty($plans) || !is_array($plans)) {
      return;
    }

    $this->database->delete('ms_fact_plan_snapshot')
      ->condition('snapshot_id', $snapshot_id)
      ->execute();

    foreach ($plans as $plan) {
      $plan_code = $this->normalizePlanCode($plan['plan_code'] ?? '');
      $plan_label = $this->formatPlanLabel($plan['plan_label'] ?? $plan_code);
      $this->ensurePlanLevelDefinition($plan_code, $plan_label);

      $this->database->insert('ms_fact_plan_snapshot')
        ->fields([
          'snapshot_id' => $snapshot_id,
          'plan_code' => $plan_code,
          'plan_label' => $plan_label,
          'count_members' => (int) ($plan['count_members'] ?? 0),
        ])
        ->execute();
    }
    $this->invalidateDatasetCache('plan_levels');
  }

  protected function importMembershipTypesSnapshot($snapshot_id, array $payload) {
    $types = $payload['types']['counts'] ?? [];
    $members_total = (int) ($payload['types']['members_total'] ?? array_sum($types));

    $terms = $this->getMembershipTypeTerms();
    $updatedTerms = $terms;
    foreach ($types as $tid => $count) {
      if (!isset($updatedTerms[$tid])) {
        $updatedTerms[$tid] = ['label' => 'Membership Type ' . $tid];
      }
    }

    if ($updatedTerms !== $terms) {
      ksort($updatedTerms);
      $this->saveMembershipTypeTerms($updatedTerms);
      $terms = $updatedTerms;
    }

    $this->persistMembershipTypeCounts($snapshot_id, $types, $terms, $members_total);
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

    $this->database->delete('ms_fact_donation_snapshot')
      ->condition('snapshot_id', (int) $snapshot_id)
      ->execute();

    if ($this->donationSnapshotHasFirstTimeColumn() && isset($metrics['first_time_donors_count'])) {
      $fields['first_time_donors_count'] = (int) $metrics['first_time_donors_count'];
    }

    $this->database->insert('ms_fact_donation_snapshot')
      ->fields($fields)
      ->execute();
  }

  protected function importDonationRangeSnapshot($snapshot_id, array $payload) {
    if (!$this->donationRangeSnapshotTableExists()) {
      return;
    }
    $ranges = $payload['ranges'] ?? [];
    if (empty($ranges) || !is_array($ranges)) {
      return;
    }

    $this->database->delete('ms_fact_donation_range_snapshot')
      ->condition('snapshot_id', $snapshot_id)
      ->execute();

    foreach ($ranges as $range) {
      $rangeKey = isset($range['range_key']) ? (string) $range['range_key'] : '';
      if ($rangeKey === '') {
        continue;
      }
      $this->database->insert('ms_fact_donation_range_snapshot')
        ->fields([
          'snapshot_id' => $snapshot_id,
          'period_year' => (int) ($range['period_year'] ?? date('Y')),
          'period_month' => (int) ($range['period_month'] ?? 0),
          'is_year_to_date' => (int) ($range['is_year_to_date'] ?? 1),
          'range_key' => $rangeKey,
          'range_label' => (string) ($range['range_label'] ?? ''),
          'min_amount' => round((float) ($range['min_amount'] ?? 0), 2),
          'max_amount' => array_key_exists('max_amount', $range) && $range['max_amount'] !== NULL
            ? round((float) $range['max_amount'], 2)
            : NULL,
          'donors_count' => (int) ($range['donors_count'] ?? 0),
          'contributions_count' => (int) ($range['contributions_count'] ?? 0),
          'total_amount' => round((float) ($range['total_amount'] ?? 0), 2),
        ])
        ->execute();
    }
  }

  protected function importEventSnapshot($snapshot_id, array $payload) {
    $this->database->delete('ms_fact_event_snapshot')
      ->condition('snapshot_id', (int) $snapshot_id)
      ->execute();

    foreach ($payload['events'] as $event) {
      $this->database->insert('ms_fact_event_snapshot')
        ->fields([
          'snapshot_id'        => (int) $snapshot_id,
          'event_id'           => (int) $event['event_id'],
          'event_title'        => $event['event_title'],
          'event_start_date'   => $event['event_start_date'],
          'registration_count' => (int) $event['registration_count'],
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
          'events_count' => (int) ($row['events_count'] ?? 0),
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

    $snapshotDate = $metrics['snapshot_date'] ?? NULL;
    $derivedYear = (int) date('Y');
    $derivedMonth = 0;
    $derivedDay = 0;
    if ($snapshotDate) {
      try {
        $date = new \DateTimeImmutable($snapshotDate);
        $derivedYear = (int) $date->format('Y');
        $derivedMonth = (int) $date->format('n');
        $derivedDay = (int) $date->format('j');
      }
      catch (\Exception $e) {
        // Ignore parsing errors; fallback values will be used.
      }
    }

    $fields = [
      'snapshot_id' => $snapshot_id,
      'period_year' => (int) ($metrics['period_year'] ?? $derivedYear),
      'period_month' => (int) ($metrics['period_month'] ?? $derivedMonth),
      'period_day' => (int) ($metrics['period_day'] ?? $derivedDay),
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

    $this->database->delete('ms_fact_survey_snapshot')
      ->condition('snapshot_id', (int) $snapshot_id)
      ->execute();

    $this->database->insert('ms_fact_survey_snapshot')
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

    $this->database->delete('ms_fact_tool_uptime_snapshot')
      ->condition('snapshot_id', (int) $snapshot_id)
      ->execute();

    $this->database->insert('ms_fact_tool_uptime_snapshot')
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

    $this->database->delete('ms_fact_donation_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_donation_range_snapshot')
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
    $this->database->delete('ms_fact_membership_type_snapshot')
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

    if (
      !$schema->fieldExists('civicrm_participant_status_type', 'is_cancelled') ||
      !$schema->fieldExists('civicrm_participant_status_type', 'is_counted')
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
    $query->condition('COALESCE(e.is_active, 0)', 1);
    $query->condition('COALESCE(e.is_template, 0)', 0);
    $query->condition('e.start_date', [$periodStart->format('Y-m-d H:i:s'), $periodEnd->format('Y-m-d H:i:s')], 'BETWEEN');

    $query->addExpression('COALESCE(ov.value, e.event_type_id)', 'event_type_id');
    $query->addExpression("COALESCE(ov.label, CONCAT('Type ', e.event_type_id))", 'event_type_label');
    $query->addExpression('COUNT(DISTINCT e.id)', 'events_count');
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
      $eventsHeld = (int) $record->events_count;
      $totalAmount = round((float) $record->total_revenue, 2);
      $label = trim((string) $record->event_type_label) !== '' ? (string) $record->event_type_label : 'Unknown';
      $eventTypeId = isset($record->event_type_id) && $record->event_type_id !== '' ? (int) $record->event_type_id : NULL;
      $average = $participants > 0 ? round($totalAmount / $participants, 2) : 0.0;

      if ($eventsHeld === 0 && $participants === 0 && $totalAmount === 0.0) {
        continue;
      }

      $this->registerEventTypeDefinition($eventTypeId ?? 0, $label);

      $metrics[] = [
        'event_type_id' => $eventTypeId,
        'event_type_label' => $label,
        'events_count' => $eventsHeld,
        'participant_count' => $participants,
        'total_amount' => $totalAmount,
        'average_ticket' => $average,
      ];
    }

    return $metrics;
  }

  /**
   * Persists rows into the event type snapshot fact table.
   */
  protected function persistEventTypeSnapshot(int $snapshotId, array $rows, int $periodYear, int $periodQuarter, int $periodMonth): void {
    $this->database->delete('ms_fact_event_type_snapshot')
      ->condition('snapshot_id', $snapshotId)
      ->execute();

    if (empty($rows)) {
      return;
    }

    foreach ($rows as $metric) {
      $this->database->insert('ms_fact_event_type_snapshot')
        ->fields([
          'snapshot_id' => $snapshotId,
          'period_year' => $periodYear,
          'period_quarter' => $periodQuarter,
          'period_month' => $periodMonth,
          'event_type_id' => $metric['event_type_id'],
          'event_type_label' => $metric['event_type_label'],
          'events_count' => (int) ($metric['events_count'] ?? 0),
          'participant_count' => (int) ($metric['participant_count'] ?? 0),
          'total_amount' => round((float) ($metric['total_amount'] ?? 0), 2),
          'average_ticket' => round((float) ($metric['average_ticket'] ?? 0), 2),
        ])
        ->execute();
    }
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
  protected function calculateDonationMetrics(\DateTimeImmutable $periodStart, \DateTimeImmutable $periodEnd, bool $includeRangeBreakdown = FALSE): array {
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
        'first_time_donors_count' => 0,
        'total_amount' => 0.0,
        'recurring_amount' => 0.0,
        'onetime_amount' => 0.0,
        'range_breakdown' => [],
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
    $this->applyContributionFilters($baseQuery, 'c');

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
        'first_time_donors_count' => 0,
        'total_amount' => 0.0,
        'recurring_amount' => 0.0,
        'onetime_amount' => 0.0,
        'range_breakdown' => [],
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
    $this->applyContributionFilters($ytdQuery, 'c');
    $ytdUniqueDonors = (int) $ytdQuery->execute()->fetchField();

    $firstTimeDonorsCount = $this->countFirstTimeDonors($periodStart, $periodEnd);
    $rangeBreakdown = $includeRangeBreakdown
      ? $this->buildDonationRangeBreakdown($yearStart, $periodEnd)
      : [];

    return [
      'donors_count' => $donorsCount,
      'ytd_unique_donors' => $ytdUniqueDonors,
      'contributions_count' => $contributionsCount,
      'recurring_contributions_count' => $recurringContributionsCount,
      'onetime_contributions_count' => $oneTimeContributionsCount,
      'recurring_donors_count' => $recurringDonorsCount,
      'onetime_donors_count' => $oneTimeDonorsCount,
      'first_time_donors_count' => $firstTimeDonorsCount,
      'total_amount' => $totalAmount,
      'recurring_amount' => $recurringAmount,
      'onetime_amount' => $oneTimeAmount,
      'range_breakdown' => $rangeBreakdown,
    ];
  }

  /**
   * Applies standard filters to a contribution query.
   */
  protected function applyContributionFilters($query, string $alias = 'c', bool $useHaving = FALSE): void {
    $query->condition("{$alias}.contribution_status_id", 1);
    $query->condition("{$alias}.is_test", 0);
    if ($useHaving) {
      $query->havingCondition("SUM(COALESCE({$alias}.total_amount, 0))", 0, '>');
    }
    else {
      $query->where("COALESCE({$alias}.total_amount, 0) > :amount_zero", [':amount_zero' => 0]);
    }
    $query->isNotNull("{$alias}.contact_id");
  }

  /**
   * Counts donors whose first successful contribution falls within the period.
   */
  protected function countFirstTimeDonors(\DateTimeImmutable $periodStart, \DateTimeImmutable $periodEnd): int {
    $schema = $this->database->schema();
    if (!$schema->tableExists('civicrm_contribution')) {
      return 0;
    }

    $start = $periodStart->format('Y-m-d H:i:s');
    $end = $periodEnd->format('Y-m-d H:i:s');

    $groupQuery = $this->database->select('civicrm_contribution', 'c');
    $groupQuery->addField('c', 'contact_id');
    $groupQuery->addExpression('MIN(c.receive_date)', 'first_gift');
    $this->applyContributionFilters($groupQuery, 'c');
    $groupQuery->groupBy('c.contact_id');
    $groupQuery->having("MIN(c.receive_date) BETWEEN :first_start AND :first_end", [
      ':first_start' => $start,
      ':first_end' => $end,
    ]);

    $outer = $this->database->select($groupQuery, 'firsts');
    $outer->addExpression('COUNT(*)', 'first_time_count');

    return (int) $outer->execute()->fetchField();
  }

  /**
   * Builds the donation range breakdown for the calendar year-to-date.
   */
  protected function buildDonationRangeBreakdown(\DateTimeImmutable $yearStart, \DateTimeImmutable $periodEnd): array {
    $schema = $this->database->schema();
    if (!$schema->tableExists('civicrm_contribution')) {
      return [];
    }

    $definitions = $this->getDonationRangeDefinitions();
    if (empty($definitions)) {
      return [];
    }

    $start = $yearStart->format('Y-m-d H:i:s');
    $end = $periodEnd->format('Y-m-d H:i:s');

    $query = $this->database->select('civicrm_contribution', 'c');
    $query->addField('c', 'contact_id');
    $query->addExpression('SUM(COALESCE(c.total_amount, 0))', 'total_amount');
    $query->addExpression('COUNT(c.id)', 'gift_count');
    $query->condition('c.receive_date', [$start, $end], 'BETWEEN');
    $this->applyContributionFilters($query, 'c');
    $query->groupBy('c.contact_id');

    $rows = $query->execute()->fetchAll();
    $buckets = $this->initializeDonationRangeBuckets($definitions);

    if (empty($rows)) {
      return array_values($buckets);
    }

    foreach ($rows as $row) {
      $amount = (float) $row->total_amount;
      $giftCount = (int) $row->gift_count;
      $rangeKey = $this->resolveDonationRangeKey($amount, $definitions);
      if (!isset($buckets[$rangeKey])) {
        $buckets[$rangeKey] = [
          'range_key' => $rangeKey,
          'range_label' => $definitions[$rangeKey]['label'] ?? $rangeKey,
          'min_amount' => (float) ($definitions[$rangeKey]['min'] ?? 0),
          'max_amount' => $definitions[$rangeKey]['max'] ?? NULL,
          'donors_count' => 0,
          'contributions_count' => 0,
          'total_amount' => 0.0,
        ];
      }
      $buckets[$rangeKey]['donors_count']++;
      $buckets[$rangeKey]['contributions_count'] += $giftCount;
      $buckets[$rangeKey]['total_amount'] += $amount;
    }

    return array_map(static function (array $bucket): array {
      $bucket['total_amount'] = round($bucket['total_amount'], 2);
      return $bucket;
    }, array_values($buckets));
  }

  /**
   * Initializes donation range buckets with zeroed stats.
   */
  protected function initializeDonationRangeBuckets(array $definitions): array {
    $buckets = [];
    foreach ($definitions as $rangeId => $info) {
      $buckets[$rangeId] = [
        'range_key' => $rangeId,
        'range_label' => $info['label'],
        'min_amount' => (float) $info['min'],
        'max_amount' => $info['max'],
        'donors_count' => 0,
        'contributions_count' => 0,
        'total_amount' => 0.0,
      ];
    }
    return $buckets;
  }

  /**
   * Resolves which donation range bucket an amount belongs to.
   */
  protected function resolveDonationRangeKey(float $amount, array $definitions): string {
    foreach ($definitions as $rangeId => $info) {
      $min = isset($info['min']) ? (float) $info['min'] : 0.0;
      $max = $info['max'];
      if ($amount < $min) {
        continue;
      }
      if ($max === NULL || $amount <= (float) $max) {
        return $rangeId;
      }
    }
    $fallback = array_key_last($definitions);
    return $fallback ?? 'unbounded';
  }

  /**
   * Determines if the donation snapshot fact table has the first-time column.
   */
  protected function donationSnapshotHasFirstTimeColumn(): bool {
    if ($this->donationSnapshotHasFirstTimeColumn !== NULL) {
      return $this->donationSnapshotHasFirstTimeColumn;
    }
    $schema = $this->database->schema();
    $this->donationSnapshotHasFirstTimeColumn = $schema->fieldExists('ms_fact_donation_snapshot', 'first_time_donors_count');
    return $this->donationSnapshotHasFirstTimeColumn;
  }

  /**
   * Checks whether the donation range snapshot table is available.
   */
  protected function donationRangeSnapshotTableExists(): bool {
    if ($this->donationRangeSnapshotAvailable !== NULL) {
      return $this->donationRangeSnapshotAvailable;
    }
    $schema = $this->database->schema();
    $this->donationRangeSnapshotAvailable = $schema->tableExists('ms_fact_donation_range_snapshot');
    return $this->donationRangeSnapshotAvailable;
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

  protected function formatSnapshotDateLabel(string $value): string {
    try {
      return (new \DateTimeImmutable($value))->format('F Y');
    }
    catch (\Exception $e) {
      return $value;
    }
  }

  protected function formatTypeLabel(string $type): string {
    $label = str_replace('_', ' ', $type);
    return ucwords($label);
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

    $snapshotDefinition = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['definition'])
      ->condition('id', $snapshot_id)
      ->execute()
      ->fetchField();

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
      $this->database->delete('ms_fact_membership_type_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();

      $this->logger->info('Deleted snapshot with ID @id.', ['@id' => $snapshot_id]);
      if ($snapshotDefinition) {
        $this->invalidateDatasetCache((string) $snapshotDefinition);
      }
    }
    catch (\Exception $e) {
      $transaction->rollBack();
      $this->logger->error('Error deleting snapshot with ID @id: @message', ['@id' => $snapshot_id, '@message' => $e->getMessage()]);
      throw $e;
    }
  }

  /**
   * Invalidates cache tags for a dataset definition.
   *
   * @param string $definition
   *   Dataset machine name.
   */
  protected function invalidateDatasetCache(string $definition): void {
    if (!isset($this->cacheTagsByDefinition[$definition])) {
      return;
    }
    Cache::invalidateTags($this->cacheTagsByDefinition[$definition]);
  }
}
