<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies member_certifications and active_access_grants snapshot datasets.
 *
 * Both datasets read from badge_request nodes — each node links a member
 * to a badge taxonomy term and carries a mutable status field ('active',
 * 'pending', etc.). Snapshotting is the only way to know how many certified
 * members existed at a past date.
 *
 * @group makerspace_snapshot
 */
class MemberCertificationsSnapshotTest extends KernelTestBase {

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
      'ms_fact_certification_snapshot',
      'ms_fact_access_snapshot',
    ]);

    $this->createBadgeRequestSchema();
  }

  /**
   * Active and pending counts are split correctly per badge.
   *
   * Fixture:
   *   - Laser Cutter (TID 100): 2 active, 1 pending member
   *   - Welding (TID 200): 0 active, 2 pending members
   *
   * Expected: two certification rows with correct active/pending split.
   */
  public function testCertificationsAggregateByBadgeAndStatus(): void {
    $this->seedBadgeData();
    $this->overrideSourceQueries($this->container->get('makerspace_snapshot.snapshot_service'));

    $this->container->get('makerspace_snapshot.snapshot_service')
      ->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['member_certifications']);

    $rows = $this->database->select('ms_fact_certification_snapshot', 'c')
      ->fields('c')
      ->orderBy('badge_tid', 'ASC')
      ->execute()
      ->fetchAllAssoc('badge_tid', \PDO::FETCH_ASSOC);

    $this->assertCount(2, $rows, 'One certification row per badge type.');

    $laser = $rows[100];
    $this->assertSame('Laser Cutter', $laser['badge_name']);
    $this->assertSame(2, (int) $laser['active_count'], 'Laser Cutter has 2 active members.');
    $this->assertSame(1, (int) $laser['pending_count'], 'Laser Cutter has 1 pending member.');

    $welding = $rows[200];
    $this->assertSame('Welding', $welding['badge_name']);
    $this->assertSame(0, (int) $welding['active_count'], 'Welding has 0 active members.');
    $this->assertSame(2, (int) $welding['pending_count'], 'Welding has 2 pending members.');
  }

  /**
   * When badge_request tables are absent, no certification rows are written.
   *
   * calculateMemberCertifications() returns [] when the node field tables
   * don't exist, so the takeSnapshot() call should succeed without error
   * and simply skip writing any fact rows.
   */
  public function testMissingBadgeTablesProducesNoCertificationRows(): void {
    // Drop the tables created by setUp so the guard condition is triggered.
    $schema = $this->database->schema();
    foreach ([
      'node__field_badge_requested',
      'node__field_badge_status',
      'node__field_member_to_badge',
      'taxonomy_term_field_data',
      'node',
    ] as $table) {
      if ($schema->tableExists($table)) {
        $schema->dropTable($table);
      }
    }

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['member_certifications']);

    $count = $this->database->select('ms_fact_certification_snapshot', 'c')
      ->countQuery()->execute()->fetchField();

    $this->assertSame(0, (int) $count, 'No certification rows written when badge tables absent.');
  }

  /**
   * Active door access grants are counted for the configured door badge TID.
   *
   * Fixture: 2 badge_request nodes for Laser Cutter (TID 100) with status
   * 'active', 1 node with status 'pending'. door_term_id config = 100.
   *
   * Expected: active_grants = 2 (pending member excluded).
   */
  public function testActiveAccessGrantsCountsActiveMembersForDoorBadge(): void {
    $this->seedBadgeData();

    \Drupal::configFactory()->getEditable('unifi_access_sync.settings')
      ->set('door_term_id', 100)
      ->save();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['active_access_grants']);

    $row = $this->database->select('ms_fact_access_snapshot', 'a')
      ->fields('a')
      ->execute()
      ->fetchAssoc();

    $this->assertNotEmpty($row, 'Access grants snapshot row was created.');
    $this->assertSame(100, (int) $row['door_badge_tid']);
    $this->assertSame(2, (int) $row['active_grants'], 'Only active badge holders are counted.');
  }

  /**
   * When door_term_id config is zero/unset, no access grants row is written.
   */
  public function testAccessGrantsSkippedWhenDoorTermIdNotConfigured(): void {
    $this->seedBadgeData();
    // Explicitly ensure the config is absent / zero.
    \Drupal::configFactory()->getEditable('unifi_access_sync.settings')
      ->set('door_term_id', 0)
      ->save();

    /** @var \Drupal\makerspace_snapshot\SnapshotService $svc */
    $svc = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->overrideSourceQueries($svc);

    $svc->takeSnapshot('monthly', FALSE, '2025-06-01', 'phpunit', ['active_access_grants']);

    $count = $this->database->select('ms_fact_access_snapshot', 'a')
      ->countQuery()->execute()->fetchField();

    $this->assertSame(0, (int) $count, 'No access snapshot written when door_term_id is 0.');
  }

  /**
   * Creates the minimal badge_request node field tables used by the queries.
   */
  protected function createBadgeRequestSchema(): void {
    $s = $this->database->schema();

    $s->createTable('node', [
      'fields' => [
        'nid' => ['type' => 'serial', 'not null' => TRUE],
        'type' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
      'primary key' => ['nid'],
    ]);

    $s->createTable('node__field_badge_requested', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_badge_requested_target_id' => ['type' => 'int', 'not null' => TRUE],
      ],
    ]);

    $s->createTable('node__field_badge_status', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_badge_status_value' => ['type' => 'varchar', 'length' => 64, 'not null' => TRUE],
      ],
    ]);

    $s->createTable('node__field_member_to_badge', [
      'fields' => [
        'entity_id' => ['type' => 'int', 'not null' => TRUE],
        'deleted' => ['type' => 'int', 'size' => 'tiny', 'not null' => TRUE, 'default' => 0],
        'field_member_to_badge_target_id' => ['type' => 'int', 'not null' => TRUE],
      ],
    ]);

    $s->createTable('taxonomy_term_field_data', [
      'fields' => [
        'tid' => ['type' => 'int', 'not null' => TRUE],
        'name' => ['type' => 'varchar', 'length' => 255, 'not null' => TRUE],
      ],
      'primary key' => ['tid'],
    ]);
  }

  /**
   * Seeds badge taxonomy terms and badge_request nodes.
   *
   * Badge terms:
   *   TID 100 — Laser Cutter
   *   TID 200 — Welding
   *
   * Nodes:
   *   NID 1: badge=100, status=active,  member=10
   *   NID 2: badge=100, status=active,  member=11
   *   NID 3: badge=100, status=pending, member=12
   *   NID 4: badge=200, status=pending, member=10
   *   NID 5: badge=200, status=pending, member=11
   */
  protected function seedBadgeData(): void {
    $this->database->insert('taxonomy_term_field_data')
      ->fields(['tid' => 100, 'name' => 'Laser Cutter'])->execute();
    $this->database->insert('taxonomy_term_field_data')
      ->fields(['tid' => 200, 'name' => 'Welding'])->execute();

    $nodes = [
      ['nid' => 1, 'badge_tid' => 100, 'status' => 'active',  'member_uid' => 10],
      ['nid' => 2, 'badge_tid' => 100, 'status' => 'active',  'member_uid' => 11],
      ['nid' => 3, 'badge_tid' => 100, 'status' => 'pending', 'member_uid' => 12],
      ['nid' => 4, 'badge_tid' => 200, 'status' => 'pending', 'member_uid' => 10],
      ['nid' => 5, 'badge_tid' => 200, 'status' => 'pending', 'member_uid' => 11],
    ];

    foreach ($nodes as $n) {
      $this->database->insert('node')
        ->fields(['nid' => $n['nid'], 'type' => 'badge_request'])->execute();
      $this->database->insert('node__field_badge_requested')
        ->fields(['entity_id' => $n['nid'], 'deleted' => 0, 'field_badge_requested_target_id' => $n['badge_tid']])->execute();
      $this->database->insert('node__field_badge_status')
        ->fields(['entity_id' => $n['nid'], 'deleted' => 0, 'field_badge_status_value' => $n['status']])->execute();
      $this->database->insert('node__field_member_to_badge')
        ->fields(['entity_id' => $n['nid'], 'deleted' => 0, 'field_member_to_badge_target_id' => $n['member_uid']])->execute();
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
