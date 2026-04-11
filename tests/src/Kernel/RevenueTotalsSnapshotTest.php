<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies that revenue_totals snapshots correctly aggregate MRR.
 *
 * The field_member_payment_monthly profile field is overwritten on each
 * Chargebee sync and has no history, so snapshotting it is the only way
 * to reconstruct MRR at a past date.
 *
 * @group makerspace_snapshot
 */
class RevenueTotalsSnapshotTest extends KernelTestBase {

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
      'ms_fact_revenue_snapshot',
    ]);

    $this->createMembershipSourceSchema();
    $this->createPaymentMonthlySchema();
  }

  /**
   * Active and paused MRR are split correctly, with the right totals.
   *
   * Fixture: 3 active members ($100, $150, $200) and 2 paused ($75, $50).
   * Expected: active_mrr=450, paused_mrr=125, total_mrr=575, avg=150.
   */
  public function testRevenueSnapshotSplitsActiveAndPaused(): void {
    $this->seedMembers();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['revenue_totals']);

    $row = $this->database->select('ms_fact_revenue_snapshot', 'r')
      ->fields('r')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row, 'Revenue snapshot row was created.');
    $this->assertSame(3, (int) $row['active_count']);
    $this->assertSame(2, (int) $row['paused_count']);
    $this->assertSame(450.00, round((float) $row['active_mrr'], 2), 'Active MRR is sum of non-paused payment fields.');
    $this->assertSame(125.00, round((float) $row['paused_mrr'], 2), 'Paused MRR excludes active members.');
    $this->assertSame(575.00, round((float) $row['total_mrr'], 2), 'Total MRR is active + paused.');
    $this->assertSame(150.00, round((float) $row['avg_monthly_active'], 2), 'Average is active_mrr / active_count.');
  }

  /**
   * A member with no payment field row contributes $0 to MRR without error.
   *
   * New members before their first Chargebee sync may have no profile field
   * row at all. The LEFT JOIN should treat the absence as $0.
   */
  public function testMemberWithNullPaymentFieldCountsAsZero(): void {
    $this->database->insert('users_field_data')->fields(['uid' => 1, 'status' => 1])->execute();
    $this->database->insert('user__roles')->fields(['entity_id' => 1, 'roles_target_id' => 'member'])->execute();
    $this->database->insert('profile')->fields(['profile_id' => 1, 'uid' => 1, 'type' => 'main'])->execute();
    // Deliberately no row in profile__field_member_payment_monthly.

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['revenue_totals']);

    $row = $this->database->select('ms_fact_revenue_snapshot', 'r')
      ->fields('r')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row);
    $this->assertSame(1, (int) $row['active_count']);
    $this->assertSame(0, (int) $row['paused_count']);
    $this->assertSame(0.00, round((float) $row['active_mrr'], 2), 'Missing payment field treated as $0.');
    $this->assertSame(0.00, round((float) $row['avg_monthly_active'], 2), 'Average is $0 when MRR is $0.');
  }

  /**
   * A member paused via manual_pause flag is excluded from active MRR.
   */
  public function testManualPauseFlagExcludesMemberFromActiveMrr(): void {
    // UID 1: active, $200/mo.
    $this->database->insert('users_field_data')->fields(['uid' => 1, 'status' => 1])->execute();
    $this->database->insert('user__roles')->fields(['entity_id' => 1, 'roles_target_id' => 'member'])->execute();
    $this->database->insert('profile')->fields(['profile_id' => 1, 'uid' => 1, 'type' => 'main'])->execute();
    $this->database->insert('profile__field_member_payment_monthly')
      ->fields(['entity_id' => 1, 'deleted' => 0, 'field_member_payment_monthly_value' => 200.00])
      ->execute();

    // UID 2: manually paused, $100/mo.
    $this->database->insert('users_field_data')->fields(['uid' => 2, 'status' => 1])->execute();
    $this->database->insert('user__roles')->fields(['entity_id' => 2, 'roles_target_id' => 'member'])->execute();
    $this->database->insert('user__field_manual_pause')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_manual_pause_value' => 1])
      ->execute();
    $this->database->insert('profile')->fields(['profile_id' => 2, 'uid' => 2, 'type' => 'main'])->execute();
    $this->database->insert('profile__field_member_payment_monthly')
      ->fields(['entity_id' => 2, 'deleted' => 0, 'field_member_payment_monthly_value' => 100.00])
      ->execute();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['revenue_totals']);

    $row = $this->database->select('ms_fact_revenue_snapshot', 'r')
      ->fields('r')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row);
    $this->assertSame(1, (int) $row['active_count']);
    $this->assertSame(1, (int) $row['paused_count']);
    $this->assertSame(200.00, round((float) $row['active_mrr'], 2));
    $this->assertSame(100.00, round((float) $row['paused_mrr'], 2));
  }

  /**
   * Creates pared-down membership source tables required by the SQL queries.
   */
  protected function createMembershipSourceSchema(): void {
    $s = $this->database->schema();

    $s->createTable('users_field_data', [
      'fields' => [
        'uid' => ['type' => 'int', 'not null' => TRUE],
        'status' => ['type' => 'int', 'not null' => TRUE, 'default' => 1],
      ],
      'primary key' => ['uid'],
    ]);

    $s->createTable('user__roles', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'roles_target_id' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
    ]);

    $s->createTable('user__field_chargebee_payment_pause', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_chargebee_payment_pause_value' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
    ]);

    $s->createTable('user__field_manual_pause', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_manual_pause_value' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
      ],
    ]);

    $s->createTable('profile', [
      'fields' => [
        'profile_id' => ['type' => 'serial', 'not null' => TRUE],
        'uid' => ['type' => 'int', 'not null' => TRUE],
        'type' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
      'primary key' => ['profile_id'],
    ]);
  }

  /**
   * Creates the profile__field_member_payment_monthly table.
   */
  protected function createPaymentMonthlySchema(): void {
    $this->database->schema()->createTable('profile__field_member_payment_monthly', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_member_payment_monthly_value' => ['type' => 'numeric', 'precision' => 10, 'scale' => 2, 'not null' => FALSE],
      ],
    ]);
  }

  /**
   * Seeds 3 active ($100, $150, $200) and 2 paused ($75, $50) members.
   */
  protected function seedMembers(): void {
    $members = [
      ['uid' => 1, 'amount' => 100.00, 'paused' => FALSE],
      ['uid' => 2, 'amount' => 150.00, 'paused' => FALSE],
      ['uid' => 3, 'amount' => 200.00, 'paused' => FALSE],
      ['uid' => 4, 'amount' => 75.00,  'paused' => TRUE,  'pause_type' => 'chargebee'],
      ['uid' => 5, 'amount' => 50.00,  'paused' => TRUE,  'pause_type' => 'manual'],
    ];

    foreach ($members as $i => $m) {
      $this->database->insert('users_field_data')->fields(['uid' => $m['uid'], 'status' => 1])->execute();
      $this->database->insert('user__roles')->fields(['entity_id' => $m['uid'], 'roles_target_id' => 'member'])->execute();
      $this->database->insert('profile')->fields(['profile_id' => $i + 1, 'uid' => $m['uid'], 'type' => 'main'])->execute();
      $this->database->insert('profile__field_member_payment_monthly')
        ->fields(['entity_id' => $i + 1, 'deleted' => 0, 'field_member_payment_monthly_value' => $m['amount']])
        ->execute();

      if (!empty($m['paused'])) {
        $table = ($m['pause_type'] ?? 'chargebee') === 'manual'
          ? 'user__field_manual_pause'
          : 'user__field_chargebee_payment_pause';
        $col = ($m['pause_type'] ?? 'chargebee') === 'manual'
          ? 'field_manual_pause_value'
          : 'field_chargebee_payment_pause_value';
        $this->database->insert($table)->fields(['entity_id' => $m['uid'], 'deleted' => 0, $col => 1])->execute();
      }
    }
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
