<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies that imports create separate rows and don't overwrite cron data.
 *
 * @group makerspace_snapshot
 */
class ImportSourceIsolationTest extends KernelTestBase {

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
      'ms_fact_donation_snapshot',
      'ms_fact_donation_range_snapshot',
    ]);

    $this->createContributionSchema();
    $this->createMembershipSourceSchema();
  }

  /**
   * Ensures an import for the same date/definition creates a new row.
   *
   * When an automatic_cron snapshot exists and a manual_import arrives for the
   * same definition + date, the cron row must remain untouched and a separate
   * manual_import row must be created.
   */
  public function testImportDoesNotOverwriteCronSnapshot(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $service */
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueriesForSqlite($service);

    // Create a cron snapshot first.
    $service->takeSnapshot(
      'monthly',
      FALSE,
      '2025-06-01',
      'automatic_cron',
      ['donation_metrics'],
      '2025-05-31'
    );

    // Verify the cron snapshot was created.
    $cronRow = $this->database->select('ms_snapshot', 's')
      ->fields('s')
      ->condition('definition', 'donation_metrics')
      ->condition('source', 'automatic_cron')
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($cronRow, 'Cron snapshot row exists.');
    $cronId = (int) $cronRow['id'];
    $cronCreatedAt = $cronRow['created_at'];

    // Now import for the same definition and date.
    // importDonationMetricsSnapshot expects data under a 'metrics' key.
    $importPayload = [
      'metrics' => [
        'period_year' => 2025,
        'period_month' => 5,
        'donors_count' => 15,
        'ytd_unique_donors' => 20,
        'contributions_count' => 18,
        'recurring_contributions_count' => 5,
        'onetime_contributions_count' => 13,
        'recurring_donors_count' => 4,
        'onetime_donors_count' => 11,
        'first_time_donors_count' => 3,
        'total_amount' => 5000.00,
        'recurring_amount' => 1500.00,
        'onetime_amount' => 3500.00,
      ],
    ];
    $importSnapshotId = $service->importSnapshot(
      'donation_metrics',
      'monthly',
      '2025-06-01',
      $importPayload
    );

    // Both rows should exist with different sources.
    $allRows = $this->database->select('ms_snapshot', 's')
      ->fields('s')
      ->condition('definition', 'donation_metrics')
      ->condition('snapshot_date', '2025-06-01')
      ->execute()
      ->fetchAll();
    $this->assertCount(2, $allRows, 'Two snapshot rows exist for same date/definition.');

    // The cron row must be unchanged.
    $cronRowAfter = $this->database->select('ms_snapshot', 's')
      ->fields('s')
      ->condition('id', $cronId)
      ->execute()
      ->fetchAssoc();
    $this->assertSame('automatic_cron', $cronRowAfter['source']);
    $this->assertSame($cronCreatedAt, $cronRowAfter['created_at'], 'Cron row created_at unchanged.');

    // The import row must have a different ID and manual_import source.
    $this->assertNotSame($cronId, (int) $importSnapshotId);
    $importRow = $this->database->select('ms_snapshot', 's')
      ->fields('s')
      ->condition('id', $importSnapshotId)
      ->execute()
      ->fetchAssoc();
    $this->assertSame('manual_import', $importRow['source']);
    $this->assertSame('2025-06-01', $importRow['snapshot_date']);

    // The import fact row should contain the imported data.
    $importFact = $this->database->select('ms_fact_donation_snapshot', 'd')
      ->fields('d')
      ->condition('snapshot_id', $importSnapshotId)
      ->execute()
      ->fetchAssoc();
    $this->assertNotEmpty($importFact, 'Import donation fact row exists.');
    $this->assertSame(15, (int) $importFact['donors_count']);
    $this->assertSame(5000.00, round((float) $importFact['total_amount'], 2));
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
   * Replaces MySQL-specific source SQL with SQLite-safe test queries.
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
