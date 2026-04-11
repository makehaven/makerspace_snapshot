<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies that storage_occupancy snapshots correctly aggregate unit data.
 *
 * The storage_unit and storage_assignment ECK entities are mutable in place —
 * unit status and assignment prices change without history. Snapshotting is
 * the only way to know past occupancy and MRR.
 *
 * @group makerspace_snapshot
 */
class StorageOccupancySnapshotTest extends KernelTestBase {

  protected Connection $database;

  protected static $modules = [
    'system',
    'user',
    'makerspace_snapshot',
  ];

  protected function setUp(): void {
    parent::setUp();

    $this->database = \Drupal::database();
    $this->installConfig(['makerspace_snapshot']);
    $this->installSchema('makerspace_snapshot', [
      'ms_snapshot',
      'ms_fact_storage_snapshot',
    ]);
  }

  /**
   * When storage_unit / storage_assignment tables are absent, no row is written.
   *
   * calculateStorageOccupancy() returns NULL when the storage tables don't
   * exist (i.e. storage_manager module not installed). takeSnapshot() should
   * succeed silently and leave ms_fact_storage_snapshot empty.
   */
  public function testStorageTablesAbsentProducesNoSnapshotRow(): void {
    // Do NOT create storage tables — this is the guard condition.
    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['storage_occupancy']);

    $count = $this->database->select('ms_fact_storage_snapshot', 's')
      ->countQuery()->execute()->fetchField();

    $this->assertSame(0, (int) $count, 'No storage snapshot row when tables are absent.');
  }

  /**
   * Occupancy rate and billed/complimentary MRR are calculated correctly.
   *
   * Fixture:
   *   Units: 3 total (2 occupied, 1 vacant).
   *   Active assignments: $150 billed, $75 complimentary.
   *
   * Expected:
   *   units_total=3, units_occupied=2, units_vacant=1,
   *   occupancy_rate=66.67,
   *   billed_mrr=150, complimentary_mrr=75, total_mrr=225,
   *   potential_mrr=0 (type/price tables absent), active_violations=0.
   */
  public function testOccupancyAndMrrCalculation(): void {
    $this->createStorageSchema();
    $this->seedStorageData();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['storage_occupancy']);

    $row = $this->database->select('ms_fact_storage_snapshot', 's')
      ->fields('s')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row, 'Storage snapshot row was created.');
    $this->assertSame(3, (int) $row['units_total']);
    $this->assertSame(2, (int) $row['units_occupied']);
    $this->assertSame(1, (int) $row['units_vacant']);
    $this->assertSame(66.67, round((float) $row['occupancy_rate'], 2), 'Occupancy rate rounds to 66.67%.');
    $this->assertSame(150.00, round((float) $row['billed_mrr'], 2), 'Billed MRR sums non-complimentary assignments.');
    $this->assertSame(75.00, round((float) $row['complimentary_mrr'], 2), 'Complimentary MRR sums flagged assignments.');
    $this->assertSame(225.00, round((float) $row['total_mrr'], 2), 'Total MRR is billed + complimentary.');
    $this->assertSame(0, (int) $row['active_violations'], 'No violations when violation table absent.');
    $this->assertSame(0.00, round((float) $row['violations_accrued'], 2));
  }

  /**
   * Violation counts and accrued amounts are recorded when the table exists.
   *
   * Fixture: 2 active assignments each flagged with a violation_start.
   * One owes $50, one owes $30.
   *
   * Expected: active_violations=2, violations_accrued=80.
   */
  public function testViolationsAreCountedWhenTableExists(): void {
    $this->createStorageSchema();
    $this->createViolationSchema();
    $this->seedStorageData();

    // Seed violations: both active assignments have a violation.
    $this->database->insert('storage_assignment__field_violation_start')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_violation_start_value' => '2025-05-01'])->execute();
    $this->database->insert('storage_assignment__field_violation_start')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_violation_start_value' => '2025-05-15'])->execute();

    $this->database->insert('storage_assignment__field_violation_total_due')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_violation_total_due_value' => 50.00])->execute();
    $this->database->insert('storage_assignment__field_violation_total_due')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_violation_total_due_value' => 30.00])->execute();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['storage_occupancy']);

    $row = $this->database->select('ms_fact_storage_snapshot', 's')
      ->fields('s')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row);
    $this->assertSame(2, (int) $row['active_violations'], 'Both active assignments with violations are counted.');
    $this->assertSame(80.00, round((float) $row['violations_accrued'], 2), 'Total due is summed across both violations.');
  }

  /**
   * Creates the minimum storage ECK tables required by the calculation.
   *
   * Does NOT create the optional type/price or violation tables so that
   * graceful-degradation paths for potential_mrr and violations are tested
   * separately.
   */
  protected function createStorageSchema(): void {
    $s = $this->database->schema();

    $s->createTable('storage_unit', [
      'fields' => [
        'id' => ['type' => 'serial', 'not null' => TRUE],
      ],
      'primary key' => ['id'],
    ]);

    $s->createTable('storage_unit__field_storage_status', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_storage_status_value' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
    ]);

    $s->createTable('storage_assignment', [
      'fields' => [
        'id' => ['type' => 'serial', 'not null' => TRUE],
      ],
      'primary key' => ['id'],
    ]);

    $s->createTable('storage_assignment__field_storage_assignment_status', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_storage_assignment_status_value' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
    ]);

    $s->createTable('storage_assignment__field_storage_price_snapshot', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_storage_price_snapshot_value' => ['type' => 'numeric', 'precision' => 10, 'scale' => 2, 'not null' => FALSE],
      ],
    ]);

    $s->createTable('storage_assignment__field_storage_complimentary', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_storage_complimentary_value' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
    ]);
  }

  /**
   * Creates optional violation tables used by the violation test.
   */
  protected function createViolationSchema(): void {
    $s = $this->database->schema();

    $s->createTable('storage_assignment__field_violation_start', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_violation_start_value' => ['type' => 'varchar', 'length' => 20, 'not null' => FALSE],
      ],
    ]);

    $s->createTable('storage_assignment__field_violation_total_due', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_violation_total_due_value' => ['type' => 'numeric', 'precision' => 10, 'scale' => 2, 'not null' => FALSE],
      ],
    ]);
  }

  /**
   * Seeds 3 units (2 occupied, 1 vacant) and 2 active assignments.
   *
   * Assignment 1: $150 billed (not complimentary).
   * Assignment 2: $75 complimentary.
   */
  protected function seedStorageData(): void {
    // 3 storage units.
    for ($i = 1; $i <= 3; $i++) {
      $this->database->insert('storage_unit')->fields(['id' => $i])->execute();
    }

    // Units 1 and 2 are occupied; unit 3 is vacant (no status row → treated as absent).
    $this->database->insert('storage_unit__field_storage_status')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_storage_status_value' => 'occupied'])->execute();
    $this->database->insert('storage_unit__field_storage_status')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_storage_status_value' => 'occupied'])->execute();
    // Unit 3 intentionally has no status row — treated as non-occupied.

    // 2 active assignments.
    $this->database->insert('storage_assignment')->fields(['id' => 1])->execute();
    $this->database->insert('storage_assignment')->fields(['id' => 2])->execute();

    $this->database->insert('storage_assignment__field_storage_assignment_status')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_storage_assignment_status_value' => 'active'])->execute();
    $this->database->insert('storage_assignment__field_storage_assignment_status')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_storage_assignment_status_value' => 'active'])->execute();

    // Assignment 1: $150 billed.
    $this->database->insert('storage_assignment__field_storage_price_snapshot')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_storage_price_snapshot_value' => 150.00])->execute();
    $this->database->insert('storage_assignment__field_storage_complimentary')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_storage_complimentary_value' => 0])->execute();

    // Assignment 2: $75 complimentary.
    $this->database->insert('storage_assignment__field_storage_price_snapshot')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_storage_price_snapshot_value' => 75.00])->execute();
    $this->database->insert('storage_assignment__field_storage_complimentary')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_storage_complimentary_value' => 1])->execute();
  }

  /**
   * Replaces MySQL-specific membership source SQL with SQLite-safe no-ops.
   */
  protected function overrideSourceQueries(object $svc): void {
    $noop = ['sql' => "SELECT 0 AS member_id, 'NONE' AS plan_code, 'None' AS plan_label WHERE 1=0"];
    $noopPeriod = ['sql' => "SELECT 0 AS member_id, 'NONE' AS plan_code, 'None' AS plan_label, '1970-01-01' AS occurred_at WHERE 1=0"];
    $prop = new \ReflectionProperty($svc, 'sourceQueries');
    $prop->setAccessible(TRUE);
    $prop->setValue($svc, [
      'sql_active' => $noop,
      'sql_paused' => $noop,
      'sql_lapsed' => $noop,
      'sql_joins' => $noopPeriod,
      'sql_cancels' => $noopPeriod,
    ]);
  }

}
