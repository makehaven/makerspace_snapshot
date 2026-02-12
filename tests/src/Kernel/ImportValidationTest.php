<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\KernelTests\KernelTestBase;

/**
 * Validates importSnapshot input validation rejects bad inputs.
 *
 * @group makerspace_snapshot
 */
class ImportValidationTest extends KernelTestBase {

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

    $this->installConfig(['makerspace_snapshot']);
    $this->installSchema('makerspace_snapshot', [
      'ms_snapshot',
      'ms_fact_donation_snapshot',
      'ms_fact_donation_range_snapshot',
    ]);
  }

  /**
   * Rejects a non-Y-m-d format string.
   */
  public function testRejectsInvalidDateFormat(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessage('Invalid snapshot_date format');
    $service->importSnapshot('donation_metrics', 'monthly', '06/01/2025', []);
  }

  /**
   * Rejects a nonsensical date that DateTimeImmutable silently adjusts.
   */
  public function testRejectsNonsensicalDate(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessage('Invalid snapshot_date');
    // Feb 30 doesn't exist; DateTimeImmutable converts it to March 2.
    $service->importSnapshot('donation_metrics', 'monthly', '2026-02-30', []);
  }

  /**
   * Rejects a completely unparseable date string.
   */
  public function testRejectsUnparseableDate(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessage('Invalid snapshot_date format');
    $service->importSnapshot('donation_metrics', 'monthly', 'not-a-date', []);
  }

  /**
   * Rejects an empty date string.
   */
  public function testRejectsEmptyDate(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $service->importSnapshot('donation_metrics', 'monthly', '', []);
  }

  /**
   * Rejects an invalid schedule value.
   */
  public function testRejectsInvalidSchedule(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessage("Invalid schedule 'biweekly'");
    $service->importSnapshot('donation_metrics', 'biweekly', '2025-06-01', []);
  }

  /**
   * Rejects an unknown definition name.
   */
  public function testRejectsInvalidDefinition(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessage('Invalid snapshot definition: nonexistent_metrics');
    $service->importSnapshot('nonexistent_metrics', 'monthly', '2025-06-01', []);
  }

  /**
   * Accepts all valid schedule values.
   */
  public function testAcceptsAllValidSchedules(): void {
    $service = $this->container->get('makerspace_snapshot.snapshot_service');
    $validSchedules = ['monthly', 'quarterly', 'annually', 'daily', 'specific'];

    // Use distinct dates to avoid unique key collision on (definition, date, source).
    $dates = [
      'monthly' => '2025-01-01',
      'quarterly' => '2025-02-01',
      'annually' => '2025-03-01',
      'daily' => '2025-04-01',
      'specific' => '2025-05-01',
    ];

    foreach ($validSchedules as $schedule) {
      // Should not throw; donation_metrics with empty payload just creates
      // the snapshot row with zero-value fact data.
      $snapshotId = $service->importSnapshot(
        'donation_metrics',
        $schedule,
        $dates[$schedule],
        [
          'period_year' => 2025,
          'period_month' => 5,
          'donors_count' => 0,
          'ytd_unique_donors' => 0,
          'contributions_count' => 0,
          'recurring_contributions_count' => 0,
          'onetime_contributions_count' => 0,
          'recurring_donors_count' => 0,
          'onetime_donors_count' => 0,
          'first_time_donors_count' => 0,
          'total_amount' => 0,
          'recurring_amount' => 0,
          'onetime_amount' => 0,
        ]
      );
      $this->assertNotEmpty($snapshotId, "Schedule '{$schedule}' produced a snapshot ID.");
    }
  }

}
