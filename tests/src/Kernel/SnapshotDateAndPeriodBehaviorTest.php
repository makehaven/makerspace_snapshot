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

    $this->installSchema('makerspace_snapshot', [
      'ms_snapshot',
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

}
