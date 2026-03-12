<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies snapshot capture date and period date behavior.
 *
 * @group makerspace_snapshot
 */
class SnapshotDateAndPeriodBehaviorTest extends KernelTestBase {

  /**
   * Active database connection for fixture setup/assertions.
   */
  protected Connection $database;

  /**
   * {@inheritdoc}
   */
  protected static $modules = [
    'system',
    'user',
    'makerspace_snapshot',
  ];

  /**
   * {@inheritdoc}
   */
  protected function setUp(): void {
    parent::setUp();

    $this->database = \Drupal::database();
    $this->installConfig(['makerspace_snapshot']);

    $this->installSchema('makerspace_snapshot', [
      'ms_snapshot',
      'ms_fact_org_snapshot',
      'ms_fact_donation_snapshot',
      'ms_fact_donation_range_snapshot',
    ]);

    $this->createMembershipSourceSchema();
    $this->createContributionSchema();
  }

  /**
   * Ensures capture date can differ from period reference date.
   */
  public function testCaptureDateAndPeriodReferenceAreIndependent(): void {
    $this->seedContributionsForPeriodTest();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueriesForSqlite($snapshotService);
    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2026-02-01',
      'phpunit',
      ['donation_metrics'],
      '2026-01-31'
    );

    $snapshotRow = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_date', 'snapshot_type', 'source'])
      ->condition('definition', 'donation_metrics')
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($snapshotRow, 'Snapshot metadata row was created.');
    $this->assertSame('2026-02-01', $snapshotRow['snapshot_date']);
    $this->assertSame('monthly', $snapshotRow['snapshot_type']);
    $this->assertSame('phpunit', $snapshotRow['source']);

    $donationFact = $this->database->select('ms_fact_donation_snapshot', 'd')
      ->fields('d')
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($donationFact, 'Donation fact row exists.');
    $this->assertSame(2026, (int) $donationFact['period_year']);
    $this->assertSame(1, (int) $donationFact['period_month']);
  }

  /**
   * Ensures duplicate guard checks normalized snapshot month.
   */
  public function testSnapshotExistsUsesNormalizedMonthDate(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueriesForSqlite($snapshotService);

    $this->assertFalse(
      $snapshotService->snapshotExists('monthly', '2026-02-15', 'automatic_cron')
    );

    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2026-02-01',
      'automatic_cron',
      ['donation_metrics'],
      '2026-01-31'
    );

    $this->assertTrue(
      $snapshotService->snapshotExists('monthly', '2026-02-20', 'automatic_cron')
    );
    $this->assertFalse(
      $snapshotService->snapshotExists('monthly', '2026-03-01', 'automatic_cron')
    );
  }

  /**
   * Ensures monthly period references use the full referenced month.
   */
  public function testMonthlyPeriodReferenceUsesFullMonth(): void {
    $this->seedMembershipJoinsForMonthlyPeriodTest();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideMembershipQueriesForSqlite($snapshotService);

    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2026-03-01',
      'phpunit',
      ['membership_totals'],
      '2026-02-28'
    );

    $snapshotRow = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['snapshot_date', 'snapshot_type', 'source'])
      ->condition('definition', 'membership_totals')
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($snapshotRow, 'Membership totals snapshot metadata row was created.');
    $this->assertSame('2026-03-01', $snapshotRow['snapshot_date']);

    $orgFact = $this->database->select('ms_fact_org_snapshot', 'o')
      ->fields('o', ['joins', 'cancels', 'net_change'])
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($orgFact, 'Membership totals fact row exists.');
    $this->assertSame(3, (int) $orgFact['joins']);
    $this->assertSame(0, (int) $orgFact['cancels']);
    $this->assertSame(3, (int) $orgFact['net_change']);
  }

  /**
   * Ensures quarterly period references use the full referenced quarter.
   */
  public function testQuarterlyPeriodReferenceUsesFullQuarter(): void {
    $this->seedMembershipJoinsForQuarterlyPeriodTest();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideMembershipQueriesForSqlite($snapshotService);

    $snapshotService->takeSnapshot(
      'quarterly',
      FALSE,
      '2026-04-01',
      'phpunit',
      ['membership_totals'],
      '2026-03-31'
    );

    $orgFact = $this->database->select('ms_fact_org_snapshot', 'o')
      ->fields('o', ['joins', 'cancels', 'net_change'])
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($orgFact, 'Quarterly membership totals fact row exists.');
    $this->assertSame(4, (int) $orgFact['joins']);
    $this->assertSame(0, (int) $orgFact['cancels']);
    $this->assertSame(4, (int) $orgFact['net_change']);
  }

  /**
   * Ensures annual period references use the full referenced year.
   */
  public function testAnnualPeriodReferenceUsesFullYear(): void {
    $this->seedMembershipJoinsForAnnualPeriodTest();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideMembershipQueriesForSqlite($snapshotService);

    $snapshotService->takeSnapshot(
      'annually',
      FALSE,
      '2026-01-01',
      'phpunit',
      ['membership_totals'],
      '2025-12-31'
    );

    $orgFact = $this->database->select('ms_fact_org_snapshot', 'o')
      ->fields('o', ['joins', 'cancels', 'net_change'])
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($orgFact, 'Annual membership totals fact row exists.');
    $this->assertSame(5, (int) $orgFact['joins']);
    $this->assertSame(0, (int) $orgFact['cancels']);
    $this->assertSame(5, (int) $orgFact['net_change']);
  }

  /**
   * Ensures rerunning a snapshot replaces fact rows for the same source/date.
   */
  public function testRerunReplacesExistingFactRows(): void {
    $this->seedMembershipJoinsForMonthlyPeriodTest();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideMembershipQueriesForSqlite($snapshotService);

    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2026-03-01',
      'phpunit',
      ['membership_totals'],
      '2026-02-28'
    );

    $snapshotId = (int) $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id'])
      ->condition('definition', 'membership_totals')
      ->condition('snapshot_date', '2026-03-01')
      ->condition('source', 'phpunit')
      ->range(0, 1)
      ->execute()
      ->fetchField();

    $this->assertGreaterThan(0, $snapshotId, 'Initial membership totals snapshot exists.');

    $firstFact = $this->database->select('ms_fact_org_snapshot', 'o')
      ->fields('o', ['joins'])
      ->condition('snapshot_id', $snapshotId)
      ->range(0, 1)
      ->execute()
      ->fetchAssoc();
    $this->assertSame(3, (int) $firstFact['joins']);

    $this->database->insert('users_field_data')
      ->fields([
        'uid' => 1005,
        'status' => 1,
        'created' => strtotime('2026-02-20 08:00:00 UTC'),
      ])
      ->execute();
    $this->database->insert('user__roles')
      ->fields([
        'entity_id' => 1005,
        'roles_target_id' => 'member',
      ])
      ->execute();

    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2026-03-01',
      'phpunit',
      ['membership_totals'],
      '2026-02-28'
    );

    $snapshotIds = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['id'])
      ->condition('definition', 'membership_totals')
      ->condition('snapshot_date', '2026-03-01')
      ->condition('source', 'phpunit')
      ->execute()
      ->fetchCol();
    $this->assertCount(1, $snapshotIds, 'Rerun reuses the existing snapshot metadata row.');
    $this->assertSame($snapshotId, (int) $snapshotIds[0], 'Rerun keeps the same snapshot ID.');

    $factRows = $this->database->select('ms_fact_org_snapshot', 'o')
      ->fields('o', ['snapshot_id', 'joins'])
      ->condition('snapshot_id', $snapshotId)
      ->execute()
      ->fetchAllAssoc('snapshot_id');
    $this->assertCount(1, $factRows, 'Exactly one fact row remains after rerun.');
    $this->assertSame(4, (int) $factRows[$snapshotId]->joins);
  }

  /**
   * Creates a pared-down civicrm_contribution schema for testing.
   */
  protected function createContributionSchema(): void {
    $schema = [
      'description' => 'CiviCRM contributions (test schema).',
      'fields' => [
        'id' => ['type' => 'serial', 'not null' => TRUE],
        'contribution_recur_id' => ['type' => 'int', 'not null' => FALSE],
        'contact_id' => ['type' => 'int', 'not null' => TRUE],
        'receive_date' => ['type' => 'varchar', 'length' => 32, 'not null' => TRUE],
        'total_amount' => ['type' => 'numeric', 'precision' => 10, 'scale' => 2, 'not null' => TRUE, 'default' => '0.00'],
        'contribution_status_id' => ['type' => 'int', 'not null' => TRUE, 'default' => 1],
        'is_test' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
      'primary key' => ['id'],
    ];

    $this->database->schema()->createTable('civicrm_contribution', $schema);
  }

  /**
   * Creates minimal source tables required by core snapshot SQL queries.
   */
  protected function createMembershipSourceSchema(): void {
    $schemaApi = $this->database->schema();

    $schemaApi->createTable('users_field_data', [
      'fields' => [
        'uid' => ['type' => 'int', 'not null' => TRUE],
        'status' => ['type' => 'int', 'not null' => TRUE, 'default' => 1],
        'created' => ['type' => 'int', 'not null' => TRUE, 'default' => 0],
      ],
      'primary key' => ['uid'],
    ]);

    $schemaApi->createTable('user__roles', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'roles_target_id' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
    ]);

    $schemaApi->createTable('user__field_chargebee_payment_pause', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_chargebee_payment_pause_value' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
    ]);

    $schemaApi->createTable('user__field_manual_pause', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_manual_pause_value' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
    ]);

    $schemaApi->createTable('user__field_user_chargebee_plan', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_user_chargebee_plan_value' => ['type' => 'varchar', 'length' => 255, 'not null' => FALSE],
      ],
    ]);

    $schemaApi->createTable('profile', [
      'fields' => [
        'profile_id' => ['type' => 'serial', 'not null' => TRUE],
        'uid' => ['type' => 'int', 'not null' => TRUE],
        'type' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
      'primary key' => ['profile_id'],
    ]);

    $schemaApi->createTable('profile__field_member_end_date', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_member_end_date_value' => ['type' => 'varchar', 'length' => 32, 'not null' => FALSE],
      ],
    ]);
  }

  /**
   * Seeds contribution rows for period-boundary assertions.
   */
  protected function seedContributionsForPeriodTest(): void {
    $this->database->insert('civicrm_contribution')
      ->fields([
        'id',
        'contact_id',
        'total_amount',
        'receive_date',
        'contribution_status_id',
        'is_test',
      ])
      ->values([
        'id' => 1,
        'contact_id' => 501,
        'total_amount' => 100.00,
        'receive_date' => '2026-01-15 10:00:00',
        'contribution_status_id' => 1,
        'is_test' => 0,
      ])
      ->values([
        'id' => 2,
        'contact_id' => 502,
        'total_amount' => 200.00,
        'receive_date' => '2026-02-01 00:00:00',
        'contribution_status_id' => 1,
        'is_test' => 0,
      ])
      ->execute();
  }

  /**
   * Seeds user records for monthly join-period assertions.
   */
  protected function seedMembershipJoinsForMonthlyPeriodTest(): void {
    $this->database->insert('users_field_data')
      ->fields(['uid', 'status', 'created'])
      ->values([
        'uid' => 1001,
        'status' => 1,
        'created' => strtotime('2026-02-01 09:00:00 UTC'),
      ])
      ->values([
        'uid' => 1002,
        'status' => 1,
        'created' => strtotime('2026-02-15 12:00:00 UTC'),
      ])
      ->values([
        'uid' => 1003,
        'status' => 1,
        'created' => strtotime('2026-02-28 18:30:00 UTC'),
      ])
      ->values([
        'uid' => 1004,
        'status' => 1,
        'created' => strtotime('2026-03-01 00:00:00 UTC'),
      ])
      ->execute();

    $this->database->insert('user__roles')
      ->fields(['entity_id', 'roles_target_id'])
      ->values(['entity_id' => 1001, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 1002, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 1003, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 1004, 'roles_target_id' => 'member'])
      ->execute();
  }

  /**
   * Seeds user records for quarterly join-period assertions.
   */
  protected function seedMembershipJoinsForQuarterlyPeriodTest(): void {
    $this->database->insert('users_field_data')
      ->fields(['uid', 'status', 'created'])
      ->values([
        'uid' => 2001,
        'status' => 1,
        'created' => strtotime('2026-01-01 00:00:00 UTC'),
      ])
      ->values([
        'uid' => 2002,
        'status' => 1,
        'created' => strtotime('2026-02-14 10:00:00 UTC'),
      ])
      ->values([
        'uid' => 2003,
        'status' => 1,
        'created' => strtotime('2026-03-31 23:59:59 UTC'),
      ])
      ->values([
        'uid' => 2004,
        'status' => 1,
        'created' => strtotime('2026-03-01 08:30:00 UTC'),
      ])
      ->values([
        'uid' => 2005,
        'status' => 1,
        'created' => strtotime('2026-04-01 00:00:00 UTC'),
      ])
      ->execute();

    $this->database->insert('user__roles')
      ->fields(['entity_id', 'roles_target_id'])
      ->values(['entity_id' => 2001, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 2002, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 2003, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 2004, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 2005, 'roles_target_id' => 'member'])
      ->execute();
  }

  /**
   * Seeds user records for annual join-period assertions.
   */
  protected function seedMembershipJoinsForAnnualPeriodTest(): void {
    $this->database->insert('users_field_data')
      ->fields(['uid', 'status', 'created'])
      ->values([
        'uid' => 3001,
        'status' => 1,
        'created' => strtotime('2025-01-01 00:00:00 UTC'),
      ])
      ->values([
        'uid' => 3002,
        'status' => 1,
        'created' => strtotime('2025-03-10 15:00:00 UTC'),
      ])
      ->values([
        'uid' => 3003,
        'status' => 1,
        'created' => strtotime('2025-07-04 12:00:00 UTC'),
      ])
      ->values([
        'uid' => 3004,
        'status' => 1,
        'created' => strtotime('2025-12-31 23:59:59 UTC'),
      ])
      ->values([
        'uid' => 3005,
        'status' => 1,
        'created' => strtotime('2025-11-11 11:11:11 UTC'),
      ])
      ->values([
        'uid' => 3006,
        'status' => 1,
        'created' => strtotime('2026-01-01 00:00:00 UTC'),
      ])
      ->execute();

    $this->database->insert('user__roles')
      ->fields(['entity_id', 'roles_target_id'])
      ->values(['entity_id' => 3001, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 3002, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 3003, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 3004, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 3005, 'roles_target_id' => 'member'])
      ->values(['entity_id' => 3006, 'roles_target_id' => 'member'])
      ->execute();
  }

  /**
   * Replaces MySQL-specific source SQL with SQLite-safe test queries.
   *
   * @param object $snapshotService
   *   Snapshot service instance under test.
   */
  protected function overrideSourceQueriesForSqlite(object $snapshotService): void {
    $queries = [
      'sql_active' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label WHERE 1=0",
      ],
      'sql_paused' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned (Paused)' AS plan_label WHERE 1=0",
      ],
      'sql_lapsed' => [
        'sql' => "SELECT 0 AS member_id, 'MEMBER_LAPSED' AS plan_code, 'Member (Lapsed)' AS plan_label WHERE 1=0",
      ],
      'sql_joins' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label, '1970-01-01' AS occurred_at WHERE 1=0",
      ],
      'sql_cancels' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label, '1970-01-01' AS occurred_at WHERE 1=0",
      ],
    ];

    $property = new \ReflectionProperty($snapshotService, 'sourceQueries');
    $property->setAccessible(TRUE);
    $property->setValue($snapshotService, $queries);
  }

  /**
   * Overrides source queries with SQLite-safe membership join SQL.
   */
  protected function overrideMembershipQueriesForSqlite(object $snapshotService): void {
    $queries = [
      'sql_active' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label WHERE 1=0",
      ],
      'sql_paused' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned (Paused)' AS plan_label WHERE 1=0",
      ],
      'sql_lapsed' => [
        'sql' => "SELECT 0 AS member_id, 'MEMBER_LAPSED' AS plan_code, 'Member (Lapsed)' AS plan_label WHERE 1=0",
      ],
      'sql_joins' => [
        'sql' => "SELECT u.uid AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label, date(u.created, 'unixepoch') AS occurred_at FROM users_field_data u INNER JOIN user__roles r ON u.uid = r.entity_id WHERE r.roles_target_id = 'member' AND datetime(u.created, 'unixepoch') BETWEEN :start AND :end",
      ],
      'sql_cancels' => [
        'sql' => "SELECT 0 AS member_id, 'UNASSIGNED' AS plan_code, 'Unassigned' AS plan_label, '1970-01-01' AS occurred_at WHERE 1=0",
      ],
    ];

    $property = new \ReflectionProperty($snapshotService, 'sourceQueries');
    $property->setAccessible(TRUE);
    $property->setValue($snapshotService, $queries);
  }

}
