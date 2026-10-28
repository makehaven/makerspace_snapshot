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
    'reminders_days_since' => 0,
    'mail_giveups_24h' => 0,
    'quiz_completions_7d' => 165,
    'watchdog_peak_hour_24h' => 388,
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

}
