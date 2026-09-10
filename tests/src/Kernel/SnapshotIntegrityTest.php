<?php

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\KernelTests\KernelTestBase;
use Drupal\makerspace_snapshot\SnapshotService;
use Psr\Log\NullLogger;

/**
 * Verifies collisions, atomic writes, false health and bounded recovery.
 *
 * @group makerspace_snapshot
 */
class SnapshotIntegrityTest extends KernelTestBase {

  /**
   * {@inheritdoc}
   */
  protected static $modules = ['system', 'user', 'makerspace_snapshot'];

  /**
   * {@inheritdoc}
   */
  protected function setUp(): void {
    parent::setUp();
    $this->installConfig(['makerspace_snapshot']);
    $this->container->get('module_handler')->loadInclude('makerspace_snapshot', 'install');
    $this->installSchema('makerspace_snapshot', array_keys(makerspace_snapshot_schema()));
  }

  /**
   * Provides a collector with deterministic source records.
   */
  private function service(): IntegritySnapshotService {
    $service = new IntegritySnapshotService(
      $this->container->get('database'), new NullLogger(),
      $this->container->get('config.factory'),
      $this->container->get('module_handler'),
      $this->container->get('entity_type.manager')
    );
    $service->fixtureQueries();
    return $service;
  }

  /**
   * Database-equivalent plan codes form one bucket without losing members.
   */
  public function testPlanCaseCollision(): void {
    $service = $this->service();
    $this->assertTrue($service->takeSnapshot('monthly', FALSE, '2026-09-01', 'automatic_cron', [
      'membership_totals', 'plan_levels',
    ]));
    $db = $this->container->get('database');
    $rows = $db->select('ms_fact_plan_snapshot', 'p')->fields('p')->execute()->fetchAll();
    $this->assertCount(1, $rows);
    $this->assertSame('provided', $rows[0]->plan_code);
    $this->assertSame(2, (int) $rows[0]->count_members);
    $this->assertSame(2, (int) $db->select('ms_fact_org_snapshot', 'o')->fields('o', ['members_active'])->execute()->fetchField());
    $this->assertGreaterThan(0, (int) $db->select('ms_snapshot', 's')->fields('s', ['completed_at'])->execute()->fetchField());
  }

  /**
   * Failure restores the previous snapshot, including its original metadata.
   */
  public function testFailedReplacementRollsBack(): void {
    $service = $this->service();
    $service->takeSnapshot('monthly', FALSE, '2026-09-01', 'automatic_cron', ['membership_totals', 'plan_levels']);
    $before = $this->facts();
    $service->failRevenue = TRUE;
    $this->assertFalse($service->takeSnapshot('monthly', FALSE, '2026-09-01', 'automatic_cron', [
      'membership_totals', 'plan_levels', 'revenue_totals',
    ]));
    $this->assertSame($before, $this->facts());
    $this->assertFalse($service->takeSnapshot('monthly', FALSE, '2026-10-01', 'automatic_cron', [
      'membership_totals', 'plan_levels', 'revenue_totals',
    ]));
    $this->assertSame($before, $this->facts(), 'A failed new period leaves no headers or facts.');
  }

  /**
   * Headers and a completion marker cannot conceal absent KPI or money facts.
   */
  public function testEmptyFactsAreUnhealthyAndOldPartialRunsArePreserved(): void {
    $service = $this->service();
    $db = $this->container->get('database');
    foreach (array_keys($service->buildDefinitions()) as $definition) {
      $db->insert('ms_snapshot')->fields([
        'definition' => $definition,
        'snapshot_type' => 'monthly',
        'snapshot_date' => '2026-09-01',
        'source' => 'automatic_cron',
        'created_at' => strtotime('2020-01-01'),
        'completed_at' => 1,
      ])->execute();
    }
    $issues = implode("\n", $service->snapshotIssues('monthly', '2026-09-01'));
    $this->assertStringContainsString('No KPI detail rows', $issues);
    $this->assertStringContainsString('revenue_totals facts', $issues);
    $this->assertStringContainsString('storage_occupancy facts', $issues);
    $this->assertFalse($service->automaticCaptureNeeded('monthly', '2026-09-01'));
    $db->update('ms_snapshot')->fields(['created_at' => time()])->execute();
    $this->assertTrue($service->automaticCaptureNeeded('monthly', '2026-09-01'));
  }

  /**
   * Recovery is additive and idempotent; current state stays absent.
   */
  public function testRecoveryPreservesOriginalFacts(): void {
    $service = $this->service();
    $db = $this->container->get('database');
    $service->takeSnapshot('monthly', FALSE, '2026-09-01', 'automatic_cron', ['membership_totals', 'plan_levels']);
    $before = $this->facts();
    $preview = $service->recoverHistoricalFacts('2026-09-01');
    $this->assertSame($before, $this->facts());
    $this->assertSame(2, $preview['kpis_from_original_org_facts']['kpi_total_active_members']);
    $applied = $service->recoverHistoricalFacts('2026-09-01', TRUE);
    $this->assertSame('applied', $applied['mode']);
    $this->assertSame($before['ms_snapshot'], $this->facts()['ms_snapshot']);
    $this->assertSame($before['ms_fact_org_snapshot'], $this->facts()['ms_fact_org_snapshot']);
    $this->assertSame($before['ms_fact_plan_snapshot'], $this->facts()['ms_fact_plan_snapshot']);
    $this->assertSame([], $service->recoverHistoricalFacts('2026-09-01', TRUE)['kpis_from_original_org_facts']);
    $this->assertSame(2, (int) $db->select('ms_fact_kpi_snapshot')->countQuery()->execute()->fetchField());
    $this->assertSame(0, (int) $db->select('ms_fact_revenue_snapshot')->countQuery()->execute()->fetchField());
  }

  /**
   * Captures exact rows to verify rollback and preservation.
   */
  private function facts(): array {
    $result = [];
    foreach (['ms_snapshot', 'ms_fact_org_snapshot', 'ms_fact_plan_snapshot', 'ms_fact_kpi_snapshot'] as $table) {
      $result[$table] = $this->container->get('database')->select($table, 't')->fields('t')->execute()->fetchAll(\PDO::FETCH_ASSOC);
    }
    return $result;
  }

}

/**
 * Uses a tiny source fixture and can fail after earlier datasets were written.
 */
class IntegritySnapshotService extends SnapshotService {

  /**
   * Whether to simulate a downstream failure.
   */
  public bool $failRevenue = FALSE;

  /**
   * Defines two members whose plan keys collide under database collation.
   */
  public function fixtureQueries(): void {
    $empty = ['sql' => "SELECT 1 member_id, 'none' plan_code WHERE 1=0"];
    $this->sourceQueries = array_fill_keys(['sql_active', 'sql_paused', 'sql_lapsed', 'sql_joins', 'sql_cancels'], $empty);
    $this->sourceQueries['sql_active']['sql'] = "SELECT 1 member_id, 'Provided' plan_code UNION ALL SELECT 2 member_id, 'provided' plan_code";
  }

  /**
   * {@inheritdoc}
   */
  protected function calculateRevenueTotals(): array {
    if ($this->failRevenue) {
      throw new \RuntimeException('Injected failure after plan facts.');
    }
    return parent::calculateRevenueTotals();
  }

}
