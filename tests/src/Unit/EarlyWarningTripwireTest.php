<?php

namespace Drupal\Tests\makerspace_snapshot\Unit;

use Drupal\makerspace_snapshot\EarlyWarning\EarlyWarningMonitor;
use Drupal\Tests\UnitTestCase;

/**
 * Tripwire evaluation and alert planning for the early-warning monitor.
 *
 * @coversDefaultClass \Drupal\makerspace_snapshot\EarlyWarning\EarlyWarningMonitor
 * @group makerspace_snapshot
 */
class EarlyWarningTripwireTest extends UnitTestCase {

  /**
   * Live readings from 2026-10-27: a normal week trips nothing.
   */
  protected const NORMAL = [
    'event_registrations_7d' => 46,
    'join_form_7d' => 6,
    'join_form_settled_7d' => 8,
    'join_form_no_account_7d' => 1,
    'join_unaccounted_pct' => 12.5,
    'new_accounts_7d' => 20,
    'new_members_7d' => 19,
    'member_snapshot_age_days' => 0,
    'chargebee_sync_log_48h' => 190,
    'door_events_24h' => 208,
    'reminder_job_hours_since' => 0,
    'mail_giveups_24h' => 0,
    'quiz_completions_7d' => 165,
    'watchdog_peak_hour_24h' => 388,
    'civicrm_custom_fields_missing_column' => 0,
  ];

  /**
   * @covers ::evaluate
   */
  public function testNormalWeekTripsNothing(): void {
    $this->assertSame([], EarlyWarningMonitor::evaluate(self::NORMAL));
  }

  /**
   * @covers ::evaluate
   */
  public function testThresholdsAreStrict(): void {
    // Exactly on the line is not tripped; one past it is.
    $this->assertSame([], EarlyWarningMonitor::evaluate(['door_events_24h' => 20.0] + self::NORMAL));
    $this->assertArrayHasKey('door_events_24h', EarlyWarningMonitor::evaluate(['door_events_24h' => 19.0] + self::NORMAL));
    $this->assertSame([], EarlyWarningMonitor::evaluate(['mail_giveups_24h' => 0.0] + self::NORMAL));
    $this->assertArrayHasKey('mail_giveups_24h', EarlyWarningMonitor::evaluate(['mail_giveups_24h' => 1.0] + self::NORMAL));
  }

  /**
   * A missing or unreadable reading never trips: "could not read" is not 0.
   *
   * @covers ::evaluate
   */
  public function testUnreadableDoesNotTrip(): void {
    $readings = self::NORMAL;
    $readings['event_registrations_7d'] = NULL;
    unset($readings['door_events_24h']);
    $this->assertSame([], EarlyWarningMonitor::evaluate($readings));
  }

  /**
   * A percentage of a handful of submissions is ignored.
   *
   * @covers ::evaluate
   */
  public function testGuardSuppressesSmallBase(): void {
    $small = ['join_form_settled_7d' => 4, 'join_unaccounted_pct' => 100.0] + self::NORMAL;
    $this->assertSame([], EarlyWarningMonitor::evaluate($small));
    $big = ['join_form_settled_7d' => 8, 'join_unaccounted_pct' => 87.5] + self::NORMAL;
    $this->assertArrayHasKey('join_unaccounted_pct', EarlyWarningMonitor::evaluate($big));
  }

  /**
   * New → posted; within the repeat window → quiet; after it → repeated.
   *
   * @covers ::plan
   */
  public function testPlanNewRepeatClear(): void {
    $day = 86400;
    $repeat = 3 * $day;
    $tripped = ['door_events_24h' => 'Door events in 24 h: 0'];

    $first = EarlyWarningMonitor::plan($tripped, [], 1000, $repeat);
    $this->assertSame(['door_events_24h'], $first['new']);
    $this->assertSame([], $first['repeat']);

    $second = EarlyWarningMonitor::plan($tripped, $first['state'], 1000 + $day, $repeat);
    $this->assertSame([], $second['new']);
    $this->assertSame([], $second['repeat']);
    $this->assertSame($first['state'], $second['state']);

    $fourth = EarlyWarningMonitor::plan($tripped, $second['state'], 1000 + $repeat, $repeat);
    $this->assertSame(['door_events_24h'], $fourth['repeat']);
    $this->assertSame(1000, $fourth['state']['door_events_24h']['since']);

    $cleared = EarlyWarningMonitor::plan([], $fourth['state'], 1000 + 4 * $day, $repeat);
    $this->assertSame(['door_events_24h'], $cleared['cleared']);
    $this->assertSame([], $cleared['state']);
  }

  /**
   * Quiet nights post nothing; unreadable probes alone do not post.
   *
   * @covers ::formatMessage
   */
  public function testFormatMessageOnlyWhenSomethingChanged(): void {
    $quiet = [
      'date' => '2026-10-27',
      'readings' => self::NORMAL,
      'tripped' => [],
      'plan' => ['new' => [], 'repeat' => [], 'cleared' => [], 'state' => []],
      'notices' => [],
      'unreadable' => ['door_events_24h'],
    ];
    $this->assertNull(EarlyWarningMonitor::formatMessage($quiet));

    $notice = ['notices' => ['New billing plan "yale-assembly" appeared']] + $quiet;
    $text = EarlyWarningMonitor::formatMessage($notice);
    $this->assertStringContainsString('Heads-up', $text);
    $this->assertStringContainsString('yale-assembly', $text);
    $this->assertStringContainsString('Could not read: door_events_24h', $text);
    // A heads-up alone never mentions the group.
    $this->assertStringNotContainsString('subteam', EarlyWarningMonitor::formatMessage($notice, 'S014AFFKAF4'));
  }

  /**
   * @staff is mentioned only when a tripwire newly trips.
   *
   * @covers ::formatMessage
   */
  public function testMentionOnlyOnNewTrip(): void {
    $base = [
      'date' => '2026-10-27',
      'readings' => ['door_events_24h' => 0.0] + self::NORMAL,
      'tripped' => ['door_events_24h' => 'Door events in 24 h: 0'],
      'notices' => [],
      'unreadable' => [],
    ];
    $state = ['door_events_24h' => ['since' => 1000, 'alerted' => 1000]];
    $new = ['plan' => ['new' => ['door_events_24h'], 'repeat' => [], 'cleared' => [], 'state' => $state]] + $base;
    $this->assertStringStartsWith('<!subteam^S014AFFKAF4> ', EarlyWarningMonitor::formatMessage($new, 'S014AFFKAF4'));
    $this->assertStringNotContainsString('subteam', EarlyWarningMonitor::formatMessage($new, ''));

    $repeat = ['plan' => ['new' => [], 'repeat' => ['door_events_24h'], 'cleared' => [], 'state' => $state]] + $base;
    $this->assertStringNotContainsString('subteam', EarlyWarningMonitor::formatMessage($repeat, 'S014AFFKAF4'));
  }

  /**
   * One custom field without its column trips the nightly wire.
   *
   * @covers ::evaluate
   */
  public function testMissingCustomFieldColumnTrips(): void {
    $this->assertArrayHasKey('civicrm_custom_fields_missing_column', EarlyWarningMonitor::evaluate(['civicrm_custom_fields_missing_column' => 1.0] + self::NORMAL));
  }

  /**
   * Hourly CiviCRM database-error wire, at the live-calibrated edge.
   *
   * 2026-11-04 outage: 3 people in the first 6 h; quiet weeks: 0–2.
   *
   * @covers ::evaluate
   */
  public function testHourlyDbErrorWire(): void {
    $wires = EarlyWarningMonitor::HOURLY_TRIPWIRES;
    $this->assertSame([], EarlyWarningMonitor::evaluate(['civicrm_db_error_people_6h' => 2.0], $wires));
    $this->assertArrayHasKey('civicrm_db_error_people_6h', EarlyWarningMonitor::evaluate(['civicrm_db_error_people_6h' => 3.0], $wires));
    // Hourly wires never leak into the nightly set, and vice versa.
    $this->assertSame([], EarlyWarningMonitor::evaluate(['civicrm_db_error_people_6h' => 50.0] + self::NORMAL));
    $this->assertSame([], EarlyWarningMonitor::evaluate(['door_events_24h' => 0.0], $wires));
  }

  /**
   * Gift card wires: always 0 / at least one cohort; NULL (page off) skips.
   *
   * @covers ::evaluate
   */
  public function testGiftCardWires(): void {
    $ok = ['jumpstart_open_gems_cohorts' => 2.0, 'gift_card_orders_without_card' => 0.0, 'gift_card_codes_on_after_use' => 0.0];
    $this->assertSame([], EarlyWarningMonitor::evaluate($ok + self::NORMAL));
    $this->assertArrayHasKey('jumpstart_open_gems_cohorts', EarlyWarningMonitor::evaluate(['jumpstart_open_gems_cohorts' => 0.0] + $ok + self::NORMAL));
    // Jumpstart page off: the probe reads NULL and the wire stays quiet.
    $this->assertSame([], EarlyWarningMonitor::evaluate(['jumpstart_open_gems_cohorts' => NULL] + $ok + self::NORMAL));
    $this->assertArrayHasKey('gift_card_orders_without_card', EarlyWarningMonitor::evaluate(['gift_card_orders_without_card' => 1.0] + $ok + self::NORMAL));
    $this->assertArrayHasKey('gift_card_codes_on_after_use', EarlyWarningMonitor::evaluate(['gift_card_codes_on_after_use' => 1.0] + $ok + self::NORMAL));
  }

}
