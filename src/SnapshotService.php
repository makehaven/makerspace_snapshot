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
      'description' => 'Finds members who joined or reactivated during the reporting window. Initial joins use field_member_join_date when populated, falling back to u.created. Reactivations use field_member_reactivation_date. Requires :start and :end parameters.',
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
       CASE
         WHEN NULLIF(jd.field_member_join_date_value, '') IS NOT NULL
           THEN jd.field_member_join_date_value
         ELSE FROM_UNIXTIME(u.created, '%Y-%m-%d')
       END AS occurred_at
FROM users_field_data u
INNER JOIN profile p ON p.uid = u.uid AND p.type = 'main'
LEFT JOIN profile__field_member_join_date jd ON jd.entity_id = p.profile_id AND jd.deleted = 0
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
WHERE (
  -- Explicit join date recorded: always trust it
  (NULLIF(jd.field_member_join_date_value, '') IS NOT NULL
   AND jd.field_member_join_date_value BETWEEN DATE_FORMAT(:start, '%Y-%m-%d') AND DATE_FORMAT(:end, '%Y-%m-%d'))
  OR
  -- No explicit join date: use account creation date, but only for current members
  -- so we don't count event-only signups
  (NULLIF(jd.field_member_join_date_value, '') IS NULL
   AND EXISTS (SELECT 1 FROM user__roles r WHERE r.entity_id = u.uid AND r.roles_target_id = 'member')
   AND u.created BETWEEN UNIX_TIMESTAMP(:start) AND UNIX_TIMESTAMP(:end))
)

UNION

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
       rd.field_member_reactivation_date_value AS occurred_at
FROM users_field_data u
INNER JOIN profile p ON p.uid = u.uid AND p.type = 'main'
INNER JOIN profile__field_member_reactivation_date rd ON rd.entity_id = p.profile_id AND rd.deleted = 0
LEFT JOIN user__field_user_chargebee_plan plan ON plan.entity_id = u.uid AND plan.deleted = 0
WHERE rd.field_member_reactivation_date_value
      BETWEEN DATE_FORMAT(:start, '%Y-%m-%d') AND DATE_FORMAT(:end, '%Y-%m-%d')
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
WHERE ed.field_member_end_date_value BETWEEN DATE_FORMAT(:start, '%Y-%m-%d') AND DATE_FORMAT(:end, '%Y-%m-%d')
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
    'survey_metrics' => [
      'label' => 'Annual Survey Metrics',
      'description' => 'Stores imported satisfaction and recommendation scores from the annual member survey.',
      'queries' => [],
    ],
    'revenue_totals' => [
      'label' => 'Membership Revenue',
      'description' => 'Aggregates current MRR from member payment fields. Ephemeral: field_member_payment_monthly is overwritten in place on each Chargebee sync.',
      'queries' => [],
    ],
    'storage_occupancy' => [
      'label' => 'Storage Occupancy',
      'description' => 'Point-in-time occupancy and MRR for storage units. Ephemeral: assignments are mutated in place with no history log.',
      'queries' => [],
    ],
    'member_certifications' => [
      'label' => 'Member Certifications',
      'description' => 'Active and pending badge certification counts by equipment type. Ephemeral: badge status fields are overwritten in place.',
      'queries' => [],
    ],
    'active_access_grants' => [
      'label' => 'Active Door Access Grants',
      'description' => 'Count of members with active door access badges at snapshot date.',
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
    'revenue_totals' => [
      'label' => 'Membership Revenue',
      'schedules' => ['monthly', 'quarterly', 'annually'],
      'headers' => [
        'snapshot_date',
        'active_count',
        'paused_count',
        'active_mrr',
        'paused_mrr',
        'total_mrr',
        'avg_monthly_active',
      ],
      'dataset_type' => 'revenue_totals',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'storage_occupancy' => [
      'label' => 'Storage Occupancy',
      'schedules' => ['monthly'],
      'headers' => [
        'snapshot_date',
        'units_total',
        'units_occupied',
        'units_vacant',
        'occupancy_rate',
        'billed_mrr',
        'complimentary_mrr',
        'total_mrr',
        'potential_mrr',
        'active_violations',
        'violations_accrued',
      ],
      'dataset_type' => 'storage_occupancy',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'member_certifications' => [
      'label' => 'Member Certifications',
      'schedules' => ['monthly'],
      'headers' => [
        'snapshot_date',
        'badge_tid',
        'badge_name',
        'active_count',
        'pending_count',
      ],
      'dataset_type' => 'member_certifications',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
    ],
    'active_access_grants' => [
      'label' => 'Active Door Access Grants',
      'schedules' => ['monthly'],
      'headers' => [
        'snapshot_date',
        'door_badge_tid',
        'active_grants',
      ],
      'dataset_type' => 'active_access_grants',
      'acquisition' => 'automated',
      'data_source' => 'Drupal SQL',
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
    return $definitions;
  }

  /**
   * Checks whether a snapshot already exists for cadence/date/source.
   *
   * Snapshot dates are normalized to the first day of the month to match
   * storage semantics in ms_snapshot.
   */
  public function snapshotExists(string $snapshot_type, string $snapshot_date, string $source = 'automatic_cron'): bool {
    $normalizedDate = (new \DateTimeImmutable($snapshot_date))->format('Y-m-01');

    $existingId = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id'])
      ->condition('snapshot_type', $snapshot_type)
      ->condition('snapshot_date', $normalizedDate)
      ->condition('source', $source)
      ->range(0, 1)
      ->execute()
      ->fetchField();

    return !empty($existingId);
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
   * @param string|null $period_reference_date
   *   Optional date used only for period-based calculations (Y-m-d format).
   *   When omitted, defaults to $snapshot_date.
   */
  public function takeSnapshot($snapshot_type, $is_test = FALSE, $snapshot_date = NULL, string $source = 'system', ?array $definitions = NULL, ?string $period_reference_date = NULL) {
    try {
      $isTest = (bool) $is_test;

      $snapshotDateInput = $snapshot_date ?? (new \DateTime())->format('Y-m-d');
      $snapshotDate = (new \DateTimeImmutable($snapshotDateInput))->format('Y-m-01');
      $periodReferenceInput = $period_reference_date ?? $snapshotDateInput;
      $periodReferenceDate = new \DateTimeImmutable($periodReferenceInput);

      $periodBounds = $this->resolvePeriodBounds($periodReferenceDate, $snapshot_type);
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
        $snapshot_id = $this->getOrCreateSnapshotId($definition, (string) $snapshot_type, $snapshotDate, $source);
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

      if (isset($snapshotIds['revenue_totals'])) {
        $revData = $this->calculateRevenueTotals();
        $this->database->insert('ms_fact_revenue_snapshot')
          ->fields([
            'snapshot_id' => $snapshotIds['revenue_totals'],
            'active_count' => $revData['active_count'],
            'paused_count' => $revData['paused_count'],
            'active_mrr' => $revData['active_mrr'],
            'paused_mrr' => $revData['paused_mrr'],
            'total_mrr' => $revData['total_mrr'],
            'avg_monthly_active' => $revData['avg_monthly_active'],
          ])->execute();
      }

      if (isset($snapshotIds['storage_occupancy'])) {
        $storageData = $this->calculateStorageOccupancy();
        if ($storageData !== NULL) {
          $this->database->insert('ms_fact_storage_snapshot')
            ->fields([
              'snapshot_id' => $snapshotIds['storage_occupancy'],
              'units_total' => $storageData['units_total'],
              'units_occupied' => $storageData['units_occupied'],
              'units_vacant' => $storageData['units_vacant'],
              'occupancy_rate' => $storageData['occupancy_rate'],
              'billed_mrr' => $storageData['billed_mrr'],
              'complimentary_mrr' => $storageData['complimentary_mrr'],
              'total_mrr' => $storageData['total_mrr'],
              'potential_mrr' => $storageData['potential_mrr'],
              'active_violations' => $storageData['active_violations'],
              'violations_accrued' => $storageData['violations_accrued'],
            ])->execute();
        }
      }

      if (isset($snapshotIds['member_certifications'])) {
        $certRows = $this->calculateMemberCertifications();
        foreach ($certRows as $cert) {
          $this->database->insert('ms_fact_certification_snapshot')
            ->fields([
              'snapshot_id' => $snapshotIds['member_certifications'],
              'badge_tid' => $cert['badge_tid'],
              'badge_name' => $cert['badge_name'],
              'active_count' => $cert['active_count'],
              'pending_count' => $cert['pending_count'],
            ])->execute();
        }
      }

      if (isset($snapshotIds['active_access_grants'])) {
        $accessData = $this->calculateActiveAccessGrants();
        if ($accessData !== NULL) {
          $this->database->insert('ms_fact_access_snapshot')
            ->fields([
              'snapshot_id' => $snapshotIds['active_access_grants'],
              'door_badge_tid' => $accessData['door_badge_tid'],
              'active_grants' => $accessData['active_grants'],
            ])->execute();
        }
      }

      $this->logger->info("Snapshots stored for {$snapshotDate} (" . implode(', ', array_keys($snapshotIds)) . ")");

      $this->pruneSnapshots();
    } catch (\Exception $e) {
      $this->logger->error('Error taking snapshot: @message', ['@message' => $e->getMessage()]);
    }
  }

  /**
   * Finds or creates a snapshot row and clears stale fact rows when reusing.
   */
  protected function getOrCreateSnapshotId(string $definition, string $snapshotType, string $snapshotDate, string $source): ?int {
    $existingId = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id'])
      ->condition('definition', $definition)
      ->condition('snapshot_date', $snapshotDate)
      ->condition('source', $source)
      ->range(0, 1)
      ->execute()
      ->fetchField();

    if ($existingId) {
      $snapshotId = (int) $existingId;
      $this->database->update('ms_snapshot')
        ->fields([
          'snapshot_type' => $snapshotType,
          'created_at' => time(),
        ])
        ->condition('id', $snapshotId)
        ->execute();
      $this->clearFactRowsForSnapshot($snapshotId);
      return $snapshotId;
    }

    $createdId = $this->database->insert('ms_snapshot')
      ->fields([
        'definition' => $definition,
        'snapshot_type' => $snapshotType,
        'snapshot_date' => $snapshotDate,
        'source' => $source,
        'created_at' => time(),
      ])
      ->execute();

    return $createdId ? (int) $createdId : NULL;
  }

  /**
   * Clears all fact table rows tied to a snapshot row ID.
   */
  protected function clearFactRowsForSnapshot(int $snapshotId): void {
    foreach ([
      'ms_fact_org_snapshot',
      'ms_fact_plan_snapshot',
      'ms_fact_donation_snapshot',
      'ms_fact_donation_range_snapshot',
      'ms_fact_survey_snapshot',
      'ms_fact_kpi_snapshot',
      'ms_fact_membership_type_snapshot',
      'ms_fact_membership_activity',
      'ms_fact_revenue_snapshot',
      'ms_fact_storage_snapshot',
      'ms_fact_certification_snapshot',
      'ms_fact_access_snapshot',
    ] as $table) {
      if ($this->database->schema()->tableExists($table)) {
        $this->database->delete($table)
          ->condition('snapshot_id', $snapshotId)
          ->execute();
      }
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

    if (isset($definitions['revenue_totals'])) {
      $header = $definitions['revenue_totals']['headers'];
      $rows = [];
      $revId = $snapshotRows['revenue_totals']->id ?? NULL;
      if ($revId) {
        $record = $this->database->select('ms_fact_revenue_snapshot', 'r')
          ->fields('r')
          ->condition('snapshot_id', $revId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $rows[] = [
            $normalizedDate,
            (int) $record['active_count'],
            (int) $record['paused_count'],
            round((float) $record['active_mrr'], 2),
            round((float) $record['paused_mrr'], 2),
            round((float) $record['total_mrr'], 2),
            round((float) $record['avg_monthly_active'], 2),
          ];
        }
      }
      $data['revenue_totals'] = [
        'filename' => 'revenue_totals.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('revenue_totals', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['storage_occupancy'])) {
      $header = $definitions['storage_occupancy']['headers'];
      $rows = [];
      $storId = $snapshotRows['storage_occupancy']->id ?? NULL;
      if ($storId && $this->database->schema()->tableExists('ms_fact_storage_snapshot')) {
        $record = $this->database->select('ms_fact_storage_snapshot', 's')
          ->fields('s')
          ->condition('snapshot_id', $storId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $rows[] = [
            $normalizedDate,
            (int) $record['units_total'],
            (int) $record['units_occupied'],
            (int) $record['units_vacant'],
            round((float) $record['occupancy_rate'], 2),
            round((float) $record['billed_mrr'], 2),
            round((float) $record['complimentary_mrr'], 2),
            round((float) $record['total_mrr'], 2),
            round((float) $record['potential_mrr'], 2),
            (int) $record['active_violations'],
            round((float) $record['violations_accrued'], 2),
          ];
        }
      }
      $data['storage_occupancy'] = [
        'filename' => 'storage_occupancy.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('storage_occupancy', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['member_certifications'])) {
      $header = $definitions['member_certifications']['headers'];
      $rows = [];
      $certId = $snapshotRows['member_certifications']->id ?? NULL;
      if ($certId && $this->database->schema()->tableExists('ms_fact_certification_snapshot')) {
        $result = $this->database->select('ms_fact_certification_snapshot', 'c')
          ->fields('c', ['badge_tid', 'badge_name', 'active_count', 'pending_count'])
          ->condition('snapshot_id', $certId)
          ->orderBy('badge_name')
          ->execute();
        foreach ($result as $record) {
          $rows[] = [
            $normalizedDate,
            (int) $record->badge_tid,
            (string) $record->badge_name,
            (int) $record->active_count,
            (int) $record->pending_count,
          ];
        }
      }
      $data['member_certifications'] = [
        'filename' => 'member_certifications.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('member_certifications', $header),
        'rows' => $rows,
      ];
    }

    if (isset($definitions['active_access_grants'])) {
      $header = $definitions['active_access_grants']['headers'];
      $rows = [];
      $accessId = $snapshotRows['active_access_grants']->id ?? NULL;
      if ($accessId && $this->database->schema()->tableExists('ms_fact_access_snapshot')) {
        $record = $this->database->select('ms_fact_access_snapshot', 'a')
          ->fields('a')
          ->condition('snapshot_id', $accessId)
          ->execute()
          ->fetchAssoc();
        if ($record) {
          $rows[] = [
            $normalizedDate,
            (int) $record['door_badge_tid'],
            (int) $record['active_grants'],
          ];
        }
      }
      $data['active_access_grants'] = [
        'filename' => 'active_access_grants.csv',
        'header' => $header,
        'label_row' => $this->getDatasetLabelRow('active_access_grants', $header),
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
      $query->fields('p', ['uid']);
      $query->fields('mt', ['field_membership_type_target_id']);
      $result = $query->execute();

      $typedUids = [];
      foreach ($result as $record) {
        $uid = (int) $record->uid;
        $tid = (int) $record->field_membership_type_target_id;
        $typedUids[$uid] = TRUE;
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

      // Bucket members with no membership type into term ID 0 ("Unassigned")
      // so the per-type sum always equals members_total.
      $untypedCount = count(array_diff($member_ids, array_keys($typedUids)));
      if ($untypedCount > 0) {
        $unassignedTid = 0;
        $counts[$unassignedTid] = ($counts[$unassignedTid] ?? 0) + $untypedCount;
        if (!isset($terms[$unassignedTid])) {
          $terms[$unassignedTid] = ['label' => 'Unassigned'];
          $configUpdated = TRUE;
        }
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

  /**
   * Imports a single snapshot from pre-processed payload data.
   *
   * @param string $definition
   *   The snapshot definition key (e.g. 'membership_totals').
   * @param string $schedule
   *   Snapshot cadence (monthly, quarterly, annually, daily, specific).
   * @param string $snapshot_date
   *   Date string in Y-m-d format (normalized to first of month).
   * @param array $payload
   *   Data payload keyed by the definition's expected structure.
   *
   * @return int
   *   The snapshot ID.
   *
   * @throws \InvalidArgumentException
   *   If inputs fail validation.
   * @throws \Exception
   *   If the snapshot row cannot be created.
   */
  public function importSnapshot($definition, $schedule, $snapshot_date, array $payload) {
    // Validate snapshot_date format.
    if (!is_string($snapshot_date) || !preg_match('/^\d{4}-\d{2}-\d{2}$/', $snapshot_date)) {
      throw new \InvalidArgumentException("Invalid snapshot_date format: expected Y-m-d, got '{$snapshot_date}'");
    }
    try {
      $parsed = new \DateTimeImmutable($snapshot_date);
      // Reject nonsensical dates like 2026-02-30 that DateTimeImmutable silently adjusts.
      if ($parsed->format('Y-m-d') !== $snapshot_date) {
        throw new \InvalidArgumentException("Invalid snapshot_date: '{$snapshot_date}' was interpreted as '{$parsed->format('Y-m-d')}'");
      }
    }
    catch (\Exception $e) {
      if ($e instanceof \InvalidArgumentException) {
        throw $e;
      }
      throw new \InvalidArgumentException("Cannot parse snapshot_date '{$snapshot_date}': {$e->getMessage()}");
    }

    // Validate schedule.
    $valid_schedules = ['monthly', 'quarterly', 'annually', 'daily', 'specific'];
    if (!in_array($schedule, $valid_schedules, TRUE)) {
      throw new \InvalidArgumentException("Invalid schedule '{$schedule}'. Must be one of: " . implode(', ', $valid_schedules));
    }

    $definitions = $this->buildDefinitions();
    if (!isset($definitions[$definition])) {
      throw new \InvalidArgumentException("Invalid snapshot definition: {$definition}");
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
      // Only refresh the timestamp; preserve existing date/type metadata.
      $this->database->update('ms_snapshot')
        ->fields([
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
      case 'survey_metrics':
        $this->importSurveyMetricsSnapshot($snapshot_id, $payload);
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
    if (empty($payload['totals']) || !is_array($payload['totals'])) {
      return;
    }
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
    if (empty($payload['types']) || !is_array($payload['types'])) {
      return;
    }
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

    $this->database->delete('ms_fact_survey_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_kpi_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();
    $this->database->delete('ms_fact_membership_type_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_membership_activity')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_revenue_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_storage_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_certification_snapshot')
      ->condition('snapshot_id', $snapshot_ids, 'IN')
      ->execute();

    $this->database->delete('ms_fact_access_snapshot')
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
      case 'monthly':
        $year = (int) $start->format('Y');
        $month = (int) $start->format('n');
        $start = $start->setDate($year, $month, 1);
        $end = $start->modify('last day of this month')->setTime(23, 59, 59);
        break;

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
    $baseQuery->addExpression('COUNT(DISTINCT CASE WHEN c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0 THEN c.id END)', 'recurring_contribution_count');
    $baseQuery->addExpression('COUNT(DISTINCT c.contact_id)', 'donor_count');
    $baseQuery->addExpression('COUNT(DISTINCT CASE WHEN c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0 THEN c.contact_id END)', 'recurring_donor_count');
    $baseQuery->addExpression('COUNT(DISTINCT CASE WHEN c.contribution_recur_id IS NULL OR c.contribution_recur_id = 0 THEN c.contact_id END)', 'onetime_donor_count');
    $baseQuery->addExpression('SUM(COALESCE(c.total_amount, 0))', 'total_amount');
    $baseQuery->addExpression('SUM(CASE WHEN c.contribution_recur_id IS NOT NULL AND c.contribution_recur_id <> 0 THEN COALESCE(c.total_amount, 0) ELSE 0 END)', 'recurring_amount');
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
    $consume = function (array $candidate) use (&$metrics): void {
      foreach ($candidate as $kpiId => $info) {
        if (!is_string($kpiId) || $kpiId === '') {
          continue;
        }
        $normalized = $this->normalizeKpiMetric($info);
        if ($normalized !== NULL && isset($normalized['value'])) {
          $metrics[$kpiId] = $normalized;
        }
      }
    };

    // Drupal's invokeAll() may return a merged KPI map directly.
    if ($this->isLikelyKpiMetricMap($results)) {
      $consume($results);
    }
    else {
      // Defensive fallback for nested result arrays.
      foreach ($results as $result) {
        if (!is_array($result) || !$this->isLikelyKpiMetricMap($result)) {
          continue;
        }
        $consume($result);
      }
    }

    return array_filter($metrics, static function ($row) {
      return $row !== NULL && isset($row['value']);
    });
  }

  /**
   * Determines whether a candidate array looks like a KPI metric map.
   *
   * Expected shape:
   *   ['kpi_id' => scalar|['value' => ..., ...], ...]
   */
  protected function isLikelyKpiMetricMap(array $candidate): bool {
    if (empty($candidate)) {
      return FALSE;
    }

    // A KPI row has a "value" key. A map has KPI IDs as keys.
    if (array_key_exists('value', $candidate)) {
      return FALSE;
    }

    $checked = 0;
    foreach ($candidate as $key => $value) {
      // KPI IDs must be strings. If we see numeric keys, this is likely 
      // Drupal invokeAll() results array containing multiple map returns.
      if (!is_string($key)) {
        return FALSE;
      }

      if (!preg_match('/^[a-z0-9_]+$/', $key)) {
        return FALSE;
      }

      if (!is_scalar($value) && !(is_array($value) && array_key_exists('value', $value))) {
        return FALSE;
      }
      $checked++;
    }

    return $checked > 0;
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
    if (!is_scalar($row['value'])) {
      return NULL;
    }
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
      $this->database->delete('ms_fact_survey_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_kpi_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_membership_type_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_revenue_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_storage_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_certification_snapshot')
        ->condition('snapshot_id', $snapshot_id)
        ->execute();
      $this->database->delete('ms_fact_access_snapshot')
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

  /**
   * Calculates MRR and member count from current Chargebee-synced payment fields.
   */
  protected function calculateRevenueTotals(): array {
    $sql = <<<SQL
SELECT
  SUM(CASE WHEN COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 0
             AND COALESCE(manual_pause.field_manual_pause_value, 0) = 0
           THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 1
             OR COALESCE(manual_pause.field_manual_pause_value, 0) = 1
           THEN 1 ELSE 0 END) AS paused_count,
  SUM(CASE WHEN COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 0
             AND COALESCE(manual_pause.field_manual_pause_value, 0) = 0
           THEN COALESCE(pmp.field_member_payment_monthly_value, 0) ELSE 0 END) AS active_mrr,
  SUM(CASE WHEN COALESCE(cb_pause.field_chargebee_payment_pause_value, 0) = 1
             OR COALESCE(manual_pause.field_manual_pause_value, 0) = 1
           THEN COALESCE(pmp.field_member_payment_monthly_value, 0) ELSE 0 END) AS paused_mrr,
  SUM(COALESCE(pmp.field_member_payment_monthly_value, 0)) AS total_mrr
FROM users_field_data u
INNER JOIN user__roles r ON u.uid = r.entity_id AND r.roles_target_id = 'member'
LEFT JOIN user__field_chargebee_payment_pause cb_pause ON cb_pause.entity_id = u.uid AND cb_pause.deleted = 0
LEFT JOIN user__field_manual_pause manual_pause ON manual_pause.entity_id = u.uid AND manual_pause.deleted = 0
LEFT JOIN profile p ON p.uid = u.uid AND p.type = 'main'
LEFT JOIN profile__field_member_payment_monthly pmp ON pmp.entity_id = p.profile_id AND pmp.deleted = 0
WHERE u.status = 1
SQL;

    $record = $this->database->query($sql)->fetchAssoc();
    $active_count = (int) ($record['active_count'] ?? 0);
    $active_mrr = round((float) ($record['active_mrr'] ?? 0), 2);

    return [
      'active_count' => $active_count,
      'paused_count' => (int) ($record['paused_count'] ?? 0),
      'active_mrr' => $active_mrr,
      'paused_mrr' => round((float) ($record['paused_mrr'] ?? 0), 2),
      'total_mrr' => round((float) ($record['total_mrr'] ?? 0), 2),
      'avg_monthly_active' => $active_count > 0 ? round($active_mrr / $active_count, 2) : 0.00,
    ];
  }

  /**
   * Calculates storage unit occupancy and MRR from storage_manager ECK entities.
   *
   * Returns NULL when storage_manager tables are not present or when any
   * required field table is missing (partial dev-environment schemas).
   * Any unexpected query error is logged and treated as a soft failure so
   * the broader snapshot run can continue.
   */
  protected function calculateStorageOccupancy(): ?array {
    $schema = $this->database->schema();

    // Base tables + field tables that are joined unconditionally below.
    $requiredTables = [
      'storage_unit',
      'storage_assignment',
      'storage_unit__field_storage_status',
      'storage_assignment__field_storage_assignment_status',
      'storage_assignment__field_storage_price_snapshot',
      'storage_assignment__field_storage_complimentary',
    ];
    foreach ($requiredTables as $table) {
      if (!$schema->tableExists($table)) {
        return NULL;
      }
    }

    try {
      // Total units.
      $total = (int) $this->database->select('storage_unit', 'su')
        ->countQuery()->execute()->fetchField();

      // Occupied units.
      $oq = $this->database->select('storage_unit', 'su');
      $oq->innerJoin('storage_unit__field_storage_status', 'ss', 'ss.entity_id = su.id AND ss.deleted = 0');
      $oq->condition('ss.field_storage_status_value', 'occupied');
      $occupied = (int) $oq->countQuery()->execute()->fetchField();

      $vacant = $total - $occupied;
      $occupancy_rate = $total > 0 ? round(($occupied / $total) * 100, 2) : 0.00;

      // Active assignment MRR.
      $aq = $this->database->select('storage_assignment', 'sa');
      $aq->innerJoin(
        'storage_assignment__field_storage_assignment_status', 'fst',
        'fst.entity_id = sa.id AND fst.deleted = 0 AND fst.field_storage_assignment_status_value = :active',
        [':active' => 'active']
      );
      $aq->leftJoin('storage_assignment__field_storage_price_snapshot', 'fprice', 'fprice.entity_id = sa.id AND fprice.deleted = 0');
      $aq->leftJoin('storage_assignment__field_storage_complimentary', 'fcomp', 'fcomp.entity_id = sa.id AND fcomp.deleted = 0');
      $aq->addField('fprice', 'field_storage_price_snapshot_value', 'monthly_price');
      $aq->addField('fcomp', 'field_storage_complimentary_value', 'is_complimentary');

      $billed_mrr = 0.0;
      $complimentary_mrr = 0.0;
      foreach ($aq->execute() as $row) {
        $price = (float) ($row->monthly_price ?? 0);
        if (!empty($row->is_complimentary)) {
          $complimentary_mrr += $price;
        }
        else {
          $billed_mrr += $price;
        }
      }

      // Potential MRR from vacant units.
      $potential_mrr = 0.0;
      if ($schema->tableExists('storage_unit__field_storage_type') && $schema->tableExists('taxonomy_term__field_monthly_price')) {
        $vq = $this->database->select('storage_unit', 'su');
        $vq->leftJoin('storage_unit__field_storage_status', 'ss', 'ss.entity_id = su.id AND ss.deleted = 0');
        $vq->leftJoin('storage_unit__field_storage_type', 'stype', 'stype.entity_id = su.id AND stype.deleted = 0');
        $vq->leftJoin('taxonomy_term__field_monthly_price', 'tp', 'tp.entity_id = stype.field_storage_type_target_id AND tp.deleted = 0');
        $vq->where('COALESCE(ss.field_storage_status_value, :vacant) != :occupied', [':vacant' => 'vacant', ':occupied' => 'occupied']);
        $vq->addExpression('COALESCE(SUM(tp.field_monthly_price_value), 0)', 'potential');
        $row = $vq->execute()->fetchAssoc();
        $potential_mrr = round((float) ($row['potential'] ?? 0), 2);
      }

      // Active violations.
      $active_violations = 0;
      $violations_accrued = 0.0;
      if (
        $schema->tableExists('storage_assignment__field_violation_start') &&
        $schema->tableExists('storage_assignment__field_violation_total_due')
      ) {
        $viol_q = $this->database->select('storage_assignment', 'sa');
        $viol_q->innerJoin(
          'storage_assignment__field_storage_assignment_status', 'fst',
          'fst.entity_id = sa.id AND fst.deleted = 0 AND fst.field_storage_assignment_status_value = :active',
          [':active' => 'active']
        );
        $viol_q->innerJoin('storage_assignment__field_violation_start', 'fvs', 'fvs.entity_id = sa.id AND fvs.deleted = 0');
        $viol_q->leftJoin('storage_assignment__field_violation_total_due', 'fvt', 'fvt.entity_id = sa.id AND fvt.deleted = 0');
        $viol_q->addExpression('COUNT(sa.id)', 'vcount');
        $viol_q->addExpression('COALESCE(SUM(fvt.field_violation_total_due_value), 0)', 'vaccrued');
        $row = $viol_q->execute()->fetchAssoc();
        $active_violations = (int) ($row['vcount'] ?? 0);
        $violations_accrued = round((float) ($row['vaccrued'] ?? 0), 2);
      }

      return [
        'units_total' => $total,
        'units_occupied' => $occupied,
        'units_vacant' => $vacant,
        'occupancy_rate' => $occupancy_rate,
        'billed_mrr' => round($billed_mrr, 2),
        'complimentary_mrr' => round($complimentary_mrr, 2),
        'total_mrr' => round($billed_mrr + $complimentary_mrr, 2),
        'potential_mrr' => $potential_mrr,
        'active_violations' => $active_violations,
        'violations_accrued' => $violations_accrued,
      ];
    }
    catch (\Exception $e) {
      $this->logger->warning('Storage occupancy calculation failed, skipping: @message', ['@message' => $e->getMessage()]);
      return NULL;
    }
  }

  /**
   * Calculates active and pending badge certification counts by badge type.
   *
   * Returns an empty array when badge_request node tables are not present.
   */
  protected function calculateMemberCertifications(): array {
    if (!$this->database->schema()->tableExists('node__field_badge_requested')) {
      return [];
    }

    $sql = <<<SQL
SELECT
  br.field_badge_requested_target_id AS badge_tid,
  td.name AS badge_name,
  bs.field_badge_status_value AS status,
  COUNT(DISTINCT fm.field_member_to_badge_target_id) AS member_count
FROM node n
INNER JOIN node__field_badge_requested br ON br.entity_id = n.nid AND br.deleted = 0
INNER JOIN node__field_badge_status bs ON bs.entity_id = n.nid AND bs.deleted = 0
INNER JOIN node__field_member_to_badge fm ON fm.entity_id = n.nid AND fm.deleted = 0
INNER JOIN taxonomy_term_field_data td ON td.tid = br.field_badge_requested_target_id
WHERE n.type = 'badge_request'
  AND bs.field_badge_status_value IN ('active', 'pending')
GROUP BY br.field_badge_requested_target_id, td.name, bs.field_badge_status_value
ORDER BY td.name, bs.field_badge_status_value
SQL;

    $rawRows = $this->database->query($sql)->fetchAll(\PDO::FETCH_ASSOC);
    $byBadge = [];
    foreach ($rawRows as $row) {
      $tid = (int) $row['badge_tid'];
      if (!isset($byBadge[$tid])) {
        $byBadge[$tid] = [
          'badge_tid' => $tid,
          'badge_name' => (string) $row['badge_name'],
          'active_count' => 0,
          'pending_count' => 0,
        ];
      }
      if ($row['status'] === 'active') {
        $byBadge[$tid]['active_count'] = (int) $row['member_count'];
      }
      elseif ($row['status'] === 'pending') {
        $byBadge[$tid]['pending_count'] = (int) $row['member_count'];
      }
    }

    return array_values($byBadge);
  }

  /**
   * Calculates the count of members with an active door access badge.
   *
   * Returns NULL when unifi_access_sync is not configured or badge tables are absent.
   */
  protected function calculateActiveAccessGrants(): ?array {
    if (!$this->database->schema()->tableExists('node__field_badge_requested')) {
      return NULL;
    }

    $doorTermId = (int) $this->configFactory->get('unifi_access_sync.settings')->get('door_term_id');
    if (!$doorTermId) {
      return NULL;
    }

    $query = $this->database->select('node', 'n');
    $query->innerJoin('node__field_badge_requested', 'br', 'br.entity_id = n.nid AND br.deleted = 0');
    $query->innerJoin('node__field_badge_status', 'bs', 'bs.entity_id = n.nid AND bs.deleted = 0');
    $query->innerJoin('node__field_member_to_badge', 'fm', 'fm.entity_id = n.nid AND fm.deleted = 0');
    $query->condition('n.type', 'badge_request');
    $query->condition('br.field_badge_requested_target_id', $doorTermId);
    $query->condition('bs.field_badge_status_value', 'active');
    $query->addExpression('COUNT(DISTINCT fm.field_member_to_badge_target_id)', 'grant_count');

    $count = (int) $query->execute()->fetchField();

    return [
      'door_badge_tid' => $doorTermId,
      'active_grants' => $count,
    ];
  }
}
