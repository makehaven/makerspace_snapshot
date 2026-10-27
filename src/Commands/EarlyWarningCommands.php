<?php

namespace Drupal\makerspace_snapshot\Commands;

use Drupal\makerspace_snapshot\EarlyWarning\EarlyWarningMonitor;
use Drush\Commands\DrushCommands;

/**
 * Drush access to the nightly early-warning readings.
 */
class EarlyWarningCommands extends DrushCommands {

  public function __construct(protected EarlyWarningMonitor $monitor) {
    parent::__construct();
  }

  /**
   * Shows today's readings next to 1 and 7 days ago, and what would trip.
   *
   * Read-only unless --record: nothing is stored and nothing is posted.
   *
   * @command makerspace-snapshot:early-warning
   * @aliases ms-early-warning
   * @option record Do the nightly run now: store readings, update tripwire state, post to Slack.
   * @usage drush makerspace-snapshot:early-warning
   * @usage drush makerspace-snapshot:early-warning --record
   */
  public function earlyWarning(array $options = ['record' => FALSE]): int {
    $result = $this->monitor->run((bool) $options['record']);
    $time = strtotime($result['date']);
    $yesterday = date('Y-m-d', $time - 86400);
    $lastWeek = date('Y-m-d', $time - 7 * 86400);
    $history = $this->monitor->history([$yesterday, $lastWeek]);

    $fmt = static fn ($v) => $v === NULL ? '-' : EarlyWarningMonitor::number((float) $v);
    $this->output()->writeln(sprintf('Early warning — %s%s', $result['date'], $options['record'] ? ' (recorded)' : ' (read-only)'));
    $this->output()->writeln(sprintf('%-28s %10s %10s %10s  %s', 'metric', 'today', '1d ago', '7d ago', 'tripwire'));
    foreach ($result['readings'] as $metric => $value) {
      $wire = EarlyWarningMonitor::TRIPWIRES[$metric] ?? NULL;
      $rule = $wire ? sprintf('%s %s', $wire['op'] === 'below' ? '<' : '>', $wire['value']) : '';
      $flag = isset($result['tripped'][$metric]) ? '  TRIPPED' : '';
      $this->output()->writeln(sprintf('%-28s %10s %10s %10s  %s%s', $metric, $fmt($value), $fmt($history[$yesterday][$metric] ?? NULL), $fmt($history[$lastWeek][$metric] ?? NULL), $rule, $flag));
    }
    foreach ($result['tripped'] as $line) {
      $this->output()->writeln('TRIPPED: ' . $line);
    }
    foreach ($result['notices'] as $notice) {
      $this->output()->writeln('NOTICE: ' . $notice);
    }
    if ($result['unreadable']) {
      $this->output()->writeln('COULD NOT READ: ' . implode(', ', $result['unreadable']));
    }
    $message = EarlyWarningMonitor::formatMessage($result);
    $this->output()->writeln($message === NULL ? 'Slack: nothing to post.' : ($options['record'] ? 'Slack: posted.' : "Slack would post:\n" . $message));
    return $result['tripped'] ? self::EXIT_FAILURE : self::EXIT_SUCCESS;
  }

}
