<?php

namespace Drupal\makerspace_snapshot\EarlyWarning;

use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\State\StateInterface;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;

/**
 * Nightly readings of the site's key flows, with Slack tripwires.
 *
 * Why this exists: the weekly workflow-health sweep (scripts/workflow-health.sh
 * in the site repo) is an audit. Someone has to run it, read ~85 lines and
 * remember last week's numbers. It kept no history, so "is 411% worse?" took
 * detective work, and four of its six alarming lines on 2026-10-27 were the
 * checks themselves being wrong. This service is the other half:
 *
 * - Every night it records one reading per metric in ms_early_warning_reading,
 *   so the weekly read can compare against last week instead of memory.
 * - A short list of tripwires — things that should never happen in a normal
 *   week — post to Slack when they trip, repeat every few days while tripped,
 *   and post once more when they clear. Thresholds were set from 26 weeks of
 *   live history (2026-04 → 2026-10); the "lowest normal" figure in each
 *   message is that history's minimum.
 * - Notices fire once per item for things a person should look at but that are
 *   not failures: a billing plan nobody has seen before, a new member whose
 *   email cannot receive mail.
 *
 * Everything here asks "did the thing happen?" rather than "was there an
 * error?": the failures that cost us most (reminders dead Apr–Jul 2026, the
 * digest stall of 2026-08-19) wrote no error at all.
 */
class EarlyWarningMonitor {

  /**
   * One row per metric per day.
   */
  public const TABLE = 'ms_early_warning_reading';

  /**
   * Site-timezone date (Y-m-d) of the last nightly run.
   */
  protected const STATE_LAST_RUN = 'makerspace_snapshot.early_warning.last_run';

  /**
   * Tripped tripwires: [key => ['since' => ts, 'alerted' => ts]].
   */
  protected const STATE_TRIPPED = 'makerspace_snapshot.early_warning.tripped';

  /**
   * Billing-plan term ids already seen. NULL until the first run seeds it.
   */
  protected const STATE_KNOWN_PLANS = 'makerspace_snapshot.early_warning.known_plans';

  /**
   * Member uids already checked for a bad email: [uid => ts].
   */
  protected const STATE_CHECKED_MEMBERS = 'makerspace_snapshot.early_warning.checked_members';

  /**
   * Hour (site timezone) after which the nightly run may start.
   *
   * Late enough that the member-success daily snapshot (which new_members_7d
   * reads) has normally run.
   */
  public const RUN_AFTER_HOUR = 6;

  /**
   * Days between reminders while a tripwire stays tripped.
   */
  public const REPEAT_DAYS = 3;

  /**
   * Readings older than this are deleted.
   */
  protected const KEEP_DAYS = 400;

  /**
   * Default channel: #infrastructure, where the access box already alerts.
   *
   * Override with makerspace_snapshot.settings:early_warning_channel.
   */
  public const DEFAULT_CHANNEL = 'CN2TD49HR';

  /**
   * Default user group mentioned when a tripwire first trips: @staff.
   *
   * Override with makerspace_snapshot.settings:early_warning_mention_group
   * (a Slack user group ID; '' for no mention).
   */
  public const DEFAULT_MENTION_GROUP = 'S014AFFKAF4';

  /**
   * The tripwires.
   *
   * 'below' trips when reading < value, 'above' when reading > value. 'guard'
   * names a reading that must be at least the given size before the tripwire
   * counts (a percentage of three submissions means nothing). 'normal' is the
   * observed range shown to the reader; 'means' is what to go and look at.
   */
  public const TRIPWIRES = [
    'event_registrations_7d' => [
      'op' => 'below', 'value' => 5,
      'title' => 'Event registrations',
      'normal' => 'lowest normal week: 16',
      'means' => 'CiviCRM event registration or payment is broken, or nothing is on the calendar.',
    ],
    'join_form_7d' => [
      'op' => 'below', 'value' => 3,
      'title' => 'Join form submissions',
      'normal' => 'lowest normal week: 6',
      'means' => 'the join page or its form is broken.',
    ],
    'join_unaccounted_pct' => [
      'op' => 'above', 'value' => 75, 'guard' => ['join_form_settled_7d', 5],
      'title' => 'Join form submissions with no website account',
      'normal' => 'normally 0–60%',
      'means' => 'people are filling in the join form but no account is being created for them.',
    ],
    'new_accounts_7d' => [
      'op' => 'below', 'value' => 3,
      'title' => 'New website accounts',
      'normal' => 'lowest normal week: 6',
      'means' => 'account creation (join, event sign-up, instructor application) is broken.',
    ],
    'new_members_7d' => [
      'op' => 'below', 'value' => 3,
      'title' => 'New members',
      'normal' => 'lowest normal week: 5',
      'means' => 'no joins, or the Chargebee → member role sync has stopped.',
    ],
    'member_snapshot_age_days' => [
      'op' => 'above', 'value' => 1,
      'title' => 'Days since the member-success snapshot ran',
      'normal' => 'normally 0',
      'means' => 'the member-success cron stopped; retention and new-member numbers are stale.',
    ],
    'chargebee_sync_log_48h' => [
      'op' => 'below', 'value' => 1,
      'title' => 'Chargebee sync activity in 48 h',
      'normal' => 'normally ~95 a day',
      'means' => 'Chargebee webhooks are not arriving: billing changes are not reaching the site.',
    ],
    'door_events_24h' => [
      'op' => 'below', 'value' => 20,
      'title' => 'Door events in 24 h',
      'normal' => 'lowest normal day: 54',
      'means' => 'door controllers or the access logger are down.',
    ],
    'reminders_days_since' => [
      'op' => 'above', 'value' => 2,
      'title' => 'Days since CiviCRM sent a scheduled reminder',
      'normal' => 'normally 0',
      'means' => 'Send Scheduled Reminders is not completing — event reminders and thank-you emails are not going out.',
    ],
    'mail_giveups_24h' => [
      'op' => 'above', 'value' => 0,
      'title' => 'Website emails given up on in 24 h',
      'normal' => 'normally 0',
      'means' => 'emails were never delivered; several in a row means the mail transport is down.',
    ],
    'quiz_completions_7d' => [
      'op' => 'below', 'value' => 5,
      'title' => 'Quiz completions',
      'normal' => 'normally 70–170 a week',
      'means' => 'the quiz module is broken (it carries a Drupal 11 patch).',
    ],
    'watchdog_peak_hour_24h' => [
      'op' => 'above', 'value' => 750,
      'title' => 'Log rows in the busiest hour (one message type)',
      'normal' => 'normally under 400',
      'means' => 'something is looping and writing to the log.',
    ],
  ];

  /**
   * Optional: makerspace_email_guard's checker, for typo-domain suggestions.
   *
   * @var object|null
   */
  protected $emailChecker = NULL;

  public function __construct(
    protected Connection $database,
    protected ConfigFactoryInterface $configFactory,
    protected LoggerInterface $logger,
    protected ClientInterface $httpClient,
    protected StateInterface $state,
    protected TimeInterface $time,
  ) {}

  /**
   * Setter injection: makerspace_email_guard is optional.
   */
  public function setEmailChecker($checker = NULL): void {
    $this->emailChecker = $checker;
  }

  /**
   * Cron entry point: runs once a day after RUN_AFTER_HOUR.
   */
  public function runIfDue(): void {
    $now = $this->time->getRequestTime();
    $today = date('Y-m-d', $now);
    if ((int) date('G', $now) < self::RUN_AFTER_HOUR || $this->state->get(self::STATE_LAST_RUN) === $today) {
      return;
    }
    // Recorded before the run, not after: a run killed half way must not
    // retry on every cron tick for the rest of the day.
    $this->state->set(self::STATE_LAST_RUN, $today);
    $this->run(TRUE);
  }

  /**
   * Takes today's readings and, with $act, records them and alerts.
   *
   * @param bool $act
   *   FALSE: read-only (nothing written, nothing posted).
   *
   * @return array
   *   ['date', 'readings', 'tripped', 'plan', 'notices', 'unreadable'].
   */
  public function run(bool $act): array {
    $now = $this->time->getRequestTime();
    $readings = $this->takeReadings();
    $unreadable = array_keys(array_filter($readings, static fn ($v) => $v === NULL));
    $tripped = self::evaluate($readings);
    $previous = $this->state->get(self::STATE_TRIPPED, []);
    $plan = self::plan($tripped, is_array($previous) ? $previous : [], $now, self::REPEAT_DAYS * 86400);
    $notices = array_merge($this->planNotices($act), $this->memberEmailNotices($act));

    $result = [
      'date' => date('Y-m-d', $now),
      'readings' => $readings,
      'tripped' => $tripped,
      'plan' => $plan,
      'notices' => $notices,
      'unreadable' => $unreadable,
    ];
    if (!$act) {
      return $result;
    }

    $this->record($result['date'], $readings, $tripped, $now);
    $this->state->set(self::STATE_TRIPPED, $plan['state']);
    foreach ($tripped as $key => $line) {
      $this->logger->warning('Early warning tripped: @line', ['@line' => $line]);
    }
    $message = self::formatMessage($result, $this->mentionGroup());
    if ($message !== NULL) {
      $this->postToSlack($message);
    }
    return $result;
  }

  /**
   * The metric queries. Each returns one number.
   *
   * 'requires' names a table the query needs; when it is missing (a local or
   * test site without CiviCRM or member_success) the reading is skipped, not
   * reported as broken.
   */
  protected function probes(): array {
    $join = "s.uri LIKE '%join-makehaven%'";
    // Submissions 1–8 days old: a day's grace for someone mid-way through.
    $settled = "s.created BETWEEN UNIX_TIMESTAMP() - 8*86400 AND UNIX_TIMESTAMP() - 86400";
    return [
      'event_registrations_7d' => ['requires' => 'civicrm_participant', 'sql' => "SELECT COUNT(*) FROM civicrm_participant WHERE register_date >= NOW() - INTERVAL 7 DAY"],
      'join_form_7d' => ['requires' => 'webform_submission', 'sql' => "SELECT COUNT(*) FROM webform_submission s WHERE $join AND s.created >= UNIX_TIMESTAMP() - 7*86400"],
      'join_form_settled_7d' => ['requires' => 'webform_submission', 'sql' => "SELECT COUNT(*) FROM webform_submission s WHERE $join AND $settled"],
      'join_form_no_account_7d' => ['requires' => 'webform_submission', 'sql' => "SELECT COUNT(*) FROM webform_submission s JOIN webform_submission_data d ON d.sid = s.sid AND d.name = 'email' WHERE $join AND $settled AND NOT EXISTS (SELECT 1 FROM users_field_data u WHERE u.mail = TRIM(d.value))"],
      'new_accounts_7d' => ['sql' => "SELECT COUNT(*) FROM users_field_data WHERE uid > 0 AND created >= UNIX_TIMESTAMP() - 7*86400"],
      'new_members_7d' => ['requires' => 'ms_member_success_snapshot', 'sql' => "SELECT COUNT(*) FROM (SELECT uid, MIN(snapshot_date) first_seen FROM ms_member_success_snapshot GROUP BY uid) t WHERE first_seen >= DATE_FORMAT(NOW() - INTERVAL 7 DAY, '%Y-%m-%d')"],
      'member_snapshot_age_days' => ['requires' => 'ms_member_success_snapshot', 'sql' => "SELECT DATEDIFF(NOW(), MAX(snapshot_date)) FROM ms_member_success_snapshot WHERE snapshot_type = 'daily'"],
      'payment_failed_now' => ['requires' => 'ms_member_success_snapshot', 'sql' => "SELECT COUNT(*) FROM ms_member_success_snapshot WHERE snapshot_type = 'daily' AND payment_failed = 1 AND snapshot_date = (SELECT MAX(snapshot_date) FROM ms_member_success_snapshot WHERE snapshot_type = 'daily')"],
      'paying_not_setup_open' => ['requires' => 'ms_billing_mismatch', 'sql' => "SELECT COUNT(*) FROM ms_billing_mismatch WHERE IFNULL(resolved_ts, 0) = 0 AND IFNULL(dismissed_ts, 0) = 0"],
      'chargebee_sync_log_48h' => ['requires' => 'watchdog', 'sql' => "SELECT COUNT(*) FROM watchdog WHERE type = 'chargebee_status_sync' AND timestamp >= UNIX_TIMESTAMP() - 2*86400"],
      'door_events_24h' => ['requires' => 'access_control_log_field_data', 'sql' => "SELECT COUNT(*) FROM access_control_log_field_data WHERE created >= UNIX_TIMESTAMP() - 86400"],
      'reminders_days_since' => ['requires' => 'civicrm_action_log', 'sql' => "SELECT DATEDIFF(NOW(), MAX(action_date_time)) FROM civicrm_action_log"],
      'mail_giveups_24h' => ['requires' => 'watchdog', 'sql' => "SELECT COUNT(*) FROM watchdog WHERE type = 'makerspace_mail_retry' AND message LIKE 'Giving up%' AND timestamp >= UNIX_TIMESTAMP() - 86400"],
      'digest_days_since' => ['requires' => 'civicrm_mailing', 'sql' => "SELECT DATEDIFF(NOW(), MAX(created_date)) FROM civicrm_mailing WHERE name LIKE 'MakeHaven Weekly Digest%'"],
      'quiz_completions_7d' => ['requires' => 'quiz_result', 'sql' => "SELECT COUNT(*) FROM quiz_result WHERE is_invalid = 0 AND time_end >= UNIX_TIMESTAMP() - 7*86400"],
      'badge_requests_7d' => ['sql' => "SELECT COUNT(*) FROM node_field_data WHERE type = 'badge_request' AND created >= UNIX_TIMESTAMP() - 7*86400"],
      'appointments_7d' => ['sql' => "SELECT COUNT(*) FROM node_field_data WHERE type = 'appointment' AND created >= UNIX_TIMESTAMP() - 7*86400"],
      'reservations_7d' => ['sql' => "SELECT COUNT(*) FROM node_field_data WHERE type = 'reservation' AND created >= UNIX_TIMESTAMP() - 7*86400"],
      'lending_checkouts_7d' => ['requires' => 'library_transaction__field_library_action', 'sql' => "SELECT COUNT(*) FROM library_transaction_field_data t JOIN library_transaction__field_library_action a ON a.entity_id = t.id WHERE a.field_library_action_value = 'withdraw' AND t.created >= UNIX_TIMESTAMP() - 7*86400"],
      'store_lines_7d' => ['requires' => 'material_transaction_field_data', 'sql' => "SELECT COUNT(*) FROM material_transaction_field_data WHERE created >= UNIX_TIMESTAMP() - 7*86400"],
      'watchdog_peak_hour_24h' => ['requires' => 'watchdog', 'sql' => "SELECT IFNULL(MAX(c), 0) FROM (SELECT COUNT(*) c FROM watchdog WHERE timestamp >= UNIX_TIMESTAMP() - 86400 AND type NOT IN ('page not found', 'access denied') GROUP BY type, FLOOR(timestamp / 3600)) t"],
    ];
  }

  /**
   * Runs every probe. A probe that throws reads NULL ("could not read").
   *
   * @return array
   *   [metric => float|null]. Skipped probes are absent.
   */
  public function takeReadings(): array {
    $schema = $this->database->schema();
    $readings = [];
    foreach ($this->probes() as $key => $probe) {
      if (!empty($probe['requires']) && !$schema->tableExists($probe['requires'])) {
        continue;
      }
      try {
        $value = $this->database->query($probe['sql'])->fetchField();
        $readings[$key] = ($value === FALSE || $value === NULL) ? NULL : (float) $value;
      }
      catch (\Throwable $e) {
        $this->logger->error('Early warning probe @key failed: @msg', ['@key' => $key, '@msg' => $e->getMessage()]);
        $readings[$key] = NULL;
      }
    }
    if (isset($readings['join_form_settled_7d'], $readings['join_form_no_account_7d'])) {
      $readings['join_unaccounted_pct'] = $readings['join_form_settled_7d'] > 0
        ? round(100 * $readings['join_form_no_account_7d'] / $readings['join_form_settled_7d'], 1)
        : 0.0;
    }
    return $readings;
  }

  /**
   * Which tripwires the readings trip.
   *
   * @return array
   *   [tripwire key => one-line description].
   */
  public static function evaluate(array $readings): array {
    $tripped = [];
    foreach (self::TRIPWIRES as $key => $wire) {
      $value = $readings[$key] ?? NULL;
      if ($value === NULL) {
        continue;
      }
      if (isset($wire['guard'])) {
        [$guardKey, $min] = $wire['guard'];
        if (($readings[$guardKey] ?? 0) < $min) {
          continue;
        }
      }
      $trips = $wire['op'] === 'below' ? $value < $wire['value'] : $value > $wire['value'];
      if ($trips) {
        $tripped[$key] = sprintf('%s: %s (%s). Likely: %s', $wire['title'], self::number($value), $wire['normal'], $wire['means']);
      }
    }
    return $tripped;
  }

  /**
   * Decides what to post given what tripped now and what was tripped before.
   *
   * @param array $tripped
   *   From evaluate().
   * @param array $previous
   *   Stored state: [key => ['since' => ts, 'alerted' => ts]].
   * @param int $now
   *   Current timestamp.
   * @param int $repeat
   *   Seconds before a still-tripped wire is posted again.
   *
   * @return array
   *   ['new' => keys, 'repeat' => keys, 'cleared' => keys, 'state' => array].
   */
  public static function plan(array $tripped, array $previous, int $now, int $repeat): array {
    $plan = ['new' => [], 'repeat' => [], 'cleared' => [], 'state' => []];
    foreach (array_keys($tripped) as $key) {
      $before = $previous[$key] ?? NULL;
      if (!is_array($before)) {
        $plan['new'][] = $key;
        $plan['state'][$key] = ['since' => $now, 'alerted' => $now];
      }
      elseif ($now - (int) ($before['alerted'] ?? 0) >= $repeat) {
        $plan['repeat'][] = $key;
        $plan['state'][$key] = ['since' => (int) $before['since'], 'alerted' => $now];
      }
      else {
        $plan['state'][$key] = $before;
      }
    }
    foreach (array_keys($previous) as $key) {
      if (!isset($tripped[$key])) {
        $plan['cleared'][] = $key;
      }
    }
    return $plan;
  }

  /**
   * Builds the Slack text, or NULL when there is nothing worth posting.
   *
   * @param array $result
   *   From run().
   * @param string $mentionGroup
   *   Slack user group ID to mention. Used only when a tripwire has newly
   *   tripped: repeats, clears and heads-ups post without it, so a mention
   *   always means "something just broke".
   */
  public static function formatMessage(array $result, string $mentionGroup = ''): ?string {
    $plan = $result['plan'];
    $sections = [];
    if ($plan['new']) {
      $sections[] = "*Newly tripped*\n" . implode("\n", array_map(static fn ($k) => '• ' . $result['tripped'][$k], $plan['new']));
    }
    if ($plan['repeat']) {
      $sections[] = "*Still tripped*\n" . implode("\n", array_map(static fn ($k) => sprintf('• %s (since %s)', $result['tripped'][$k], date('M j', $plan['state'][$k]['since'])), $plan['repeat']));
    }
    if ($plan['cleared']) {
      $sections[] = "*Cleared*\n" . implode("\n", array_map(static function ($k) use ($result) {
        $title = self::TRIPWIRES[$k]['title'] ?? $k;
        $value = $result['readings'][$k] ?? NULL;
        return '• ' . $title . ($value === NULL ? '' : ': back to ' . self::number($value));
      }, $plan['cleared']));
    }
    if ($result['notices']) {
      $sections[] = "*Heads-up*\n" . implode("\n", array_map(static fn ($n) => '• ' . $n, $result['notices']));
    }
    if (!$sections) {
      return NULL;
    }
    // Unreadable probes ride along with a real post rather than causing one:
    // a broken query is the weekly sweep's job, not a page.
    if ($result['unreadable']) {
      $sections[] = '_Could not read: ' . implode(', ', $result['unreadable']) . '_';
    }
    $sections[] = '_Readings and history: `drush makerspace-snapshot:early-warning`_';
    $mention = ($plan['new'] && $mentionGroup !== '') ? '<!subteam^' . $mentionGroup . '> ' : '';
    return $mention . ':rotating_light: *Website early warning — ' . $result['date'] . "*\n" . implode("\n\n", $sections);
  }

  /**
   * Billing plans nobody has seen before.
   *
   * A plan appearing is normal when staff set one up in Chargebee, and a
   * surprise when they did not (2026-10-26: a "yale-assembly" plan arrived and
   * 7 people joined on it within a day; the sweep found it, nothing announced
   * it). The first run seeds the known list silently.
   */
  protected function planNotices(bool $act): array {
    if (!$this->database->schema()->tableExists('taxonomy_term_field_data')) {
      return [];
    }
    $plans = $this->database->query("SELECT tid, name FROM {taxonomy_term_field_data} WHERE vid = 'billing_plan'")->fetchAllKeyed();
    $known = $this->state->get(self::STATE_KNOWN_PLANS);
    if (!is_array($known)) {
      if ($act) {
        $this->state->set(self::STATE_KNOWN_PLANS, array_map('intval', array_keys($plans)));
      }
      return [];
    }
    $notices = [];
    $hasPlanField = $this->database->schema()->tableExists('user__field_user_chargebee_plan');
    foreach ($plans as $tid => $name) {
      if (in_array((int) $tid, $known, TRUE)) {
        continue;
      }
      $count = $hasPlanField ? (int) $this->database->query("SELECT COUNT(*) FROM {user__field_user_chargebee_plan} WHERE field_user_chargebee_plan_value = :name", [':name' => $name])->fetchField() : 0;
      $notices[] = sprintf('New billing plan "%s" appeared from Chargebee (%d %s on it so far). Expected if staff just set it up; otherwise check Chargebee.', $name, $count, $count === 1 ? 'account' : 'accounts');
      $known[] = (int) $tid;
    }
    if ($act && $notices) {
      $this->state->set(self::STATE_KNOWN_PLANS, $known);
    }
    return $notices;
  }

  /**
   * New members whose email cannot receive mail.
   *
   * Each member is checked once. Two signals: CiviCRM has put their primary
   * email on hold (it bounced), or the domain is a near-miss of a big
   * provider (2026-10-23: a paying member joined as …@gamil.com;
   * makerspace_email_guard only suggests, by design, so nothing stopped it).
   * Members created in the last 14 days are looked at; the welcome and
   * orientation emails need a working address in that window.
   */
  protected function memberEmailNotices(bool $act): array {
    $schema = $this->database->schema();
    if (!$schema->tableExists('user__roles')) {
      return [];
    }
    $checked = $this->state->get(self::STATE_CHECKED_MEMBERS, []);
    $checked = is_array($checked) ? $checked : [];
    $now = $this->time->getRequestTime();
    $rows = $this->database->query("SELECT u.uid, u.mail, u.created FROM {users_field_data} u JOIN {user__roles} r ON r.entity_id = u.uid AND r.roles_target_id = 'member' WHERE u.created >= :since", [':since' => $now - 14 * 86400])->fetchAll();
    $civi = $schema->tableExists('civicrm_uf_match') && $schema->tableExists('civicrm_email');

    $notices = [];
    foreach ($rows as $row) {
      $uid = (int) $row->uid;
      if (isset($checked[$uid])) {
        continue;
      }
      // A day's grace: CiviCRM needs a bounce to come back before on_hold
      // means anything, and the contact may not exist yet.
      if ($now - (int) $row->created < 86400) {
        continue;
      }
      $checked[$uid] = $now;
      $mail = (string) $row->mail;
      $problem = NULL;
      if ($civi && $this->database->query("SELECT 1 FROM {civicrm_uf_match} uf JOIN {civicrm_email} e ON e.contact_id = uf.contact_id AND e.is_primary = 1 WHERE uf.uf_id = :uid AND e.on_hold > 0", [':uid' => $uid])->fetchField()) {
        $problem = 'their email bounced and CiviCRM has put it on hold';
      }
      elseif ($this->emailChecker && method_exists($this->emailChecker, 'suggest') && ($suggestion = $this->emailChecker->suggest($mail))) {
        $problem = sprintf('it looks like a typo for %s', $suggestion);
      }
      if ($problem) {
        $notices[] = sprintf('New member uid %d signed up as %s — %s, so welcome and orientation emails will not arrive. Fix it at /user/%d/edit.', $uid, $mail, $problem, $uid);
      }
    }
    if ($act) {
      // Forget members long past the window so the list stays small.
      $checked = array_filter($checked, static fn ($ts) => $ts >= $now - 30 * 86400);
      $this->state->set(self::STATE_CHECKED_MEMBERS, $checked);
    }
    return $notices;
  }

  /**
   * Stores today's readings (a rerun the same day overwrites).
   */
  protected function record(string $date, array $readings, array $tripped, int $now): void {
    if (!$this->database->schema()->tableExists(self::TABLE)) {
      return;
    }
    foreach ($readings as $metric => $value) {
      $this->database->merge(self::TABLE)
        ->keys(['reading_date' => $date, 'metric' => $metric])
        ->fields([
          'value' => $value,
          'tripped' => isset($tripped[$metric]) ? 1 : 0,
          'created' => $now,
        ])
        ->execute();
    }
    $this->database->delete(self::TABLE)
      ->condition('reading_date', date('Y-m-d', $now - self::KEEP_DAYS * 86400), '<')
      ->execute();
  }

  /**
   * Stored readings for one metric set: [date => [metric => value]].
   */
  public function history(array $dates): array {
    if (!$dates || !$this->database->schema()->tableExists(self::TABLE)) {
      return [];
    }
    $out = [];
    $rows = $this->database->select(self::TABLE, 'r')
      ->fields('r', ['reading_date', 'metric', 'value'])
      ->condition('reading_date', $dates, 'IN')
      ->execute();
    foreach ($rows as $row) {
      $out[$row->reading_date][$row->metric] = $row->value === NULL ? NULL : (float) $row->value;
    }
    return $out;
  }

  /**
   * The user group to mention when a tripwire newly trips.
   */
  protected function mentionGroup(): string {
    $group = $this->configFactory->get('makerspace_snapshot.settings')->get('early_warning_mention_group');
    return $group === NULL ? self::DEFAULT_MENTION_GROUP : trim((string) $group);
  }

  /**
   * Posts with slack_member_sync's bot token (chat.postMessage).
   *
   * Not slack_connector's incoming webhook: a modern webhook is locked to the
   * one channel it was created for and silently ignores a channel override.
   *
   * Live only, checked explicitly. Test and dev run this same cron against a
   * cloned live database and would post duplicate alerts, and the bot token
   * is NOT blank on test (verified 2026-10-27, despite settings.php's
   * non-live blanking block), so the token cannot be the gate. Readings are
   * still recorded everywhere.
   */
  protected function postToSlack(string $text): void {
    if (getenv('PANTHEON_ENVIRONMENT') !== 'live') {
      $this->logger->info('Early warning: not live, Slack post skipped.');
      return;
    }
    $token = trim((string) $this->configFactory->get('slack_member_sync.settings')->get('bot_token'));
    if ($token === '') {
      $this->logger->error('Early warning: no Slack bot token on live; alert not posted.');
      return;
    }
    $channel = trim((string) $this->configFactory->get('makerspace_snapshot.settings')->get('early_warning_channel')) ?: self::DEFAULT_CHANNEL;
    $payload = [
      'channel' => $channel,
      'text' => $text,
      'unfurl_links' => FALSE,
      'unfurl_media' => FALSE,
    ];
    try {
      $data = $this->slackCall('chat.postMessage', $payload, $token);
      if (($data['error'] ?? '') === 'not_in_channel') {
        $this->slackCall('conversations.join', ['channel' => $channel], $token);
        $data = $this->slackCall('chat.postMessage', $payload, $token);
      }
      if (empty($data['ok'])) {
        $this->logger->error('Early warning Slack post failed: @err', ['@err' => $data['error'] ?? 'unknown']);
      }
    }
    catch (\Throwable $e) {
      $this->logger->error('Early warning Slack post failed: @msg', ['@msg' => $e->getMessage()]);
    }
  }

  /**
   * Calls one Slack Web API method and returns the decoded body.
   */
  protected function slackCall(string $method, array $params, string $token): array {
    $response = $this->httpClient->request('POST', 'https://slack.com/api/' . $method, [
      'headers' => ['Authorization' => 'Bearer ' . $token],
      'json' => $params,
      'timeout' => 10,
    ]);
    return json_decode((string) $response->getBody(), TRUE) ?: [];
  }

  /**
   * Formats a reading without a pointless ".0".
   */
  public static function number(float $value): string {
    return floor($value) == $value ? (string) (int) $value : (string) round($value, 1);
  }

}
