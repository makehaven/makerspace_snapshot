<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\KernelTests\KernelTestBase;

/**
 * Ensures donation snapshots capture CiviCRM contribution data.
 *
 * @group makerspace_snapshot
 */
class DonationMetricsSnapshotTest extends KernelTestBase {

  /**
   * Active database connection for fixture setup/assertions.
   */
  protected \Drupal\Core\Database\Connection $database;

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
    $this->seedContributions();
  }

  /**
   * Validates that donation snapshots summarize contributions.
   */
  public function testDonationSnapshotGeneration(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $snapshotService */
    $snapshotService = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueriesForSqlite($snapshotService);

    $snapshotService->takeSnapshot(
      'monthly',
      FALSE,
      '2024-06-01',
      'phpunit',
      ['donation_metrics', 'donation_range_metrics']
    );

    $donationFact = $this->database->select('ms_fact_donation_snapshot', 'd')
      ->fields('d')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($donationFact, 'Donation snapshot row exists.');
    $this->assertSame(2, (int) $donationFact['donors_count']);
    $this->assertSame(2, (int) $donationFact['ytd_unique_donors']);
    $this->assertSame(2, (int) $donationFact['contributions_count']);
    $this->assertSame(1, (int) $donationFact['recurring_contributions_count']);
    $this->assertSame(1, (int) $donationFact['onetime_contributions_count']);
    $this->assertSame(1, (int) $donationFact['recurring_donors_count']);
    $this->assertSame(1, (int) $donationFact['onetime_donors_count']);
    $this->assertSame(1, (int) $donationFact['first_time_donors_count']);
    $this->assertSame(750.00, round((float) $donationFact['total_amount'], 2));

    $rangeFacts = $this->database->select('ms_fact_donation_range_snapshot', 'r')
      ->fields('r')
      ->orderBy('range_key')
      ->execute()
      ->fetchAllAssoc('range_key');

    $this->assertNotEmpty($rangeFacts, 'Donation range rows exist.');
    $this->assertArrayHasKey('100_249', $rangeFacts);
    $this->assertSame(1, (int) $rangeFacts['100_249']->donors_count);
    $this->assertSame(150.00, round((float) $rangeFacts['100_249']->total_amount, 2));
    $this->assertArrayHasKey('500_999', $rangeFacts);
    $this->assertSame(1, (int) $rangeFacts['500_999']->donors_count);
    $this->assertSame(600.00, round((float) $rangeFacts['500_999']->total_amount, 2));
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
   * Seeds the civicrm_contribution table with representative data.
   */
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

  protected function seedContributions(): void {
    $this->database->insert('civicrm_contribution')->fields([
      'id',
      'contribution_recur_id',
      'contact_id',
      'total_amount',
      'receive_date',
      'contribution_status_id',
      'is_test',
    ])
      // Historical contribution outside June period and 2024 YTD.
      ->values([
        'id' => 1,
        'contribution_recur_id' => NULL,
        'contact_id' => 101,
        'total_amount' => 95.00,
        'receive_date' => '2023-12-15 10:00:00',
        'contribution_status_id' => 1,
        'is_test' => 0,
      ])
      // Recurring contribution in June period.
      ->values([
        'id' => 2,
        'contribution_recur_id' => 77,
        'contact_id' => 101,
        'total_amount' => 150.00,
        'receive_date' => '2024-06-05 12:00:00',
        'contribution_status_id' => 1,
        'is_test' => 0,
      ])
      // One-time contribution in June period (first-time donor).
      ->values([
        'id' => 3,
        'contribution_recur_id' => NULL,
        'contact_id' => 202,
        'total_amount' => 600.00,
        'receive_date' => '2024-06-15 09:30:00',
        'contribution_status_id' => 1,
        'is_test' => 0,
      ])
      // Filtered out: non-completed status (status_id=2).
      ->values([
        'id' => 4,
        'contribution_recur_id' => NULL,
        'contact_id' => 303,
        'total_amount' => 50.00,
        'receive_date' => '2024-06-20 08:00:00',
        'contribution_status_id' => 2,
        'is_test' => 0,
      ])
      // Filtered out: test contribution.
      ->values([
        'id' => 5,
        'contribution_recur_id' => NULL,
        'contact_id' => 404,
        'total_amount' => 75.00,
        'receive_date' => '2024-06-25 14:00:00',
        'contribution_status_id' => 1,
        'is_test' => 1,
      ])
      ->execute();
  }

}
