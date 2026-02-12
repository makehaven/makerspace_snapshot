<?php

declare(strict_types=1);

namespace Drupal\Tests\makerspace_snapshot\Kernel;

use Drupal\Core\Database\Connection;
use Drupal\KernelTests\KernelTestBase;

/**
 * Verifies event type metrics dedup merges duplicate labels on import.
 *
 * @group makerspace_snapshot
 */
class EventTypeDedupTest extends KernelTestBase {

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
      'ms_fact_event_type_snapshot',
    ]);
  }

  /**
   * Ensures duplicate event_type_labels are merged by summing metrics.
   */
  public function testDuplicateLabelsAreMerged(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $service */
    $service = $this->container->get('makerspace_snapshot.snapshot_service');

    $payload = [
      'event_types' => [
        [
          'period_year' => 2025,
          'period_quarter' => 2,
          'period_month' => 6,
          'event_type_id' => 1,
          'event_type_label' => 'Workshop',
          'events_count' => 3,
          'participant_count' => 30,
          'total_amount' => 600.00,
          'average_ticket' => 20.00,
        ],
        // Duplicate label from a different source event_type_id.
        [
          'period_year' => 2025,
          'period_quarter' => 2,
          'period_month' => 6,
          'event_type_id' => 5,
          'event_type_label' => 'Workshop',
          'events_count' => 2,
          'participant_count' => 20,
          'total_amount' => 400.00,
          'average_ticket' => 20.00,
        ],
        // Unique label, should pass through unchanged.
        [
          'period_year' => 2025,
          'period_quarter' => 2,
          'period_month' => 6,
          'event_type_id' => 10,
          'event_type_label' => 'Open House',
          'events_count' => 1,
          'participant_count' => 50,
          'total_amount' => 0.00,
          'average_ticket' => 0.00,
        ],
      ],
    ];

    $snapshotId = $service->importSnapshot(
      'event_type_metrics',
      'monthly',
      '2025-07-01',
      $payload
    );

    $rows = $this->database->select('ms_fact_event_type_snapshot', 'et')
      ->fields('et')
      ->condition('snapshot_id', $snapshotId)
      ->orderBy('event_type_label')
      ->execute()
      ->fetchAllAssoc('event_type_label');

    // Should be 2 rows (merged Workshop + Open House), not 3.
    $this->assertCount(2, $rows, 'Duplicate labels were merged into one row.');

    // Verify Open House is unchanged.
    $this->assertArrayHasKey('Open House', $rows);
    $openHouse = $rows['Open House'];
    $this->assertSame(1, (int) $openHouse->events_count);
    $this->assertSame(50, (int) $openHouse->participant_count);
    $this->assertSame(0.00, round((float) $openHouse->total_amount, 2));

    // Verify Workshop is the merged result.
    $this->assertArrayHasKey('Workshop', $rows);
    $workshop = $rows['Workshop'];
    $this->assertSame(5, (int) $workshop->events_count, '3 + 2 = 5 events.');
    $this->assertSame(50, (int) $workshop->participant_count, '30 + 20 = 50 participants.');
    $this->assertSame(1000.00, round((float) $workshop->total_amount, 2), '600 + 400 = 1000.');
    // Average ticket is recomputed: 1000 / 50 = 20.00.
    $this->assertSame(20.00, round((float) $workshop->average_ticket, 2));
  }

  /**
   * Ensures average_ticket is correctly recomputed when merging.
   */
  public function testAverageTicketRecomputedOnMerge(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $service */
    $service = $this->container->get('makerspace_snapshot.snapshot_service');

    $payload = [
      'event_types' => [
        [
          'period_year' => 2025,
          'period_quarter' => 3,
          'period_month' => 9,
          'event_type_id' => 1,
          'event_type_label' => 'Class',
          'events_count' => 2,
          'participant_count' => 10,
          'total_amount' => 500.00,
          'average_ticket' => 50.00,
        ],
        [
          'period_year' => 2025,
          'period_quarter' => 3,
          'period_month' => 9,
          'event_type_id' => 2,
          'event_type_label' => 'Class',
          'events_count' => 1,
          'participant_count' => 40,
          'total_amount' => 1000.00,
          'average_ticket' => 25.00,
        ],
      ],
    ];

    $snapshotId = $service->importSnapshot(
      'event_type_metrics',
      'monthly',
      '2025-10-01',
      $payload
    );

    $row = $this->database->select('ms_fact_event_type_snapshot', 'et')
      ->fields('et')
      ->condition('snapshot_id', $snapshotId)
      ->condition('event_type_label', 'Class')
      ->execute()
      ->fetchObject();

    $this->assertNotEmpty($row);
    $this->assertSame(3, (int) $row->events_count);
    $this->assertSame(50, (int) $row->participant_count);
    $this->assertSame(1500.00, round((float) $row->total_amount, 2));
    // Recomputed: 1500 / 50 = 30.00 (not the average of 50 and 25).
    $this->assertSame(30.00, round((float) $row->average_ticket, 2));
  }

  /**
   * Ensures empty event_types payload is gracefully handled.
   */
  public function testEmptyPayloadDoesNotInsertRows(): void {
    /** @var \Drupal\makerspace_snapshot\SnapshotService $service */
    $service = $this->container->get('makerspace_snapshot.snapshot_service');

    $snapshotId = $service->importSnapshot(
      'event_type_metrics',
      'monthly',
      '2025-08-01',
      ['event_types' => []]
    );

    $count = (int) $this->database->select('ms_fact_event_type_snapshot', 'et')
      ->condition('snapshot_id', $snapshotId)
      ->countQuery()
      ->execute()
      ->fetchField();

    $this->assertSame(0, $count, 'No fact rows for empty payload.');
  }

}
