<?php

namespace Drupal\makerspace_snapshot\Service;

use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\State\StateInterface;
use Drupal\makerspace_snapshot\SnapshotService;
use GuzzleHttp\ClientInterface;
use Psr\Log\LoggerInterface;

/**
 * Verifies that snapshot runs are complete and KPI values look sane.
 *
 * Snapshot failures are otherwise silent: takeSnapshot() logs and moves on,
 * and a metric whose upstream source breaks simply records 0. This service
 * runs after the monthly snapshot window and raises issues to the log and,
 * when the Slack Connector webhook is configured, to Slack.
 */
class SnapshotHealthMonitor {

  /**
   * KPI zero-drop detection: prior consecutive nonzero months required.
   */
  const ZERO_DROP_LOOKBACK = 3;

  /**
   * State key remembering the last period+fingerprint that was reported.
   */
  const STATE_KEY = 'makerspace_snapshot.health_last_report';

  protected Connection $database;
  protected SnapshotService $snapshotService;
  protected ConfigFactoryInterface $configFactory;
  protected LoggerInterface $logger;
  protected ClientInterface $httpClient;
  protected StateInterface $state;

  public function __construct(Connection $database, SnapshotService $snapshot_service, ConfigFactoryInterface $config_factory, LoggerInterface $logger, ClientInterface $http_client, StateInterface $state) {
    $this->database = $database;
    $this->snapshotService = $snapshot_service;
    $this->configFactory = $config_factory;
    $this->logger = $logger;
    $this->httpClient = $http_client;
    $this->state = $state;
  }

  /**
   * Runs all health checks for a monthly snapshot period.
   *
   * @param string|null $period
   *   Snapshot date to check (YYYY-MM-01). Defaults to the current month.
   *
   * @return string[]
   *   Human-readable issue descriptions. Empty when healthy.
   */
  public function runChecks(?string $period = NULL): array {
    $period = $period ?: date('Y-m-01');
    $issues = [];

    $issues = array_merge($issues, $this->checkDefinitionCoverage($period));
    $issues = array_merge($issues, $this->checkKpiZeroDrops($period));
    $issues = array_merge($issues, $this->checkOrgTotals($period));

    return $issues;
  }

  /**
   * Runs checks and reports issues once per period/fingerprint.
   *
   * @return string[]
   *   The issues found (already-reported issues are still returned).
   */
  public function checkAndNotify(?string $period = NULL): array {
    $period = $period ?: date('Y-m-01');
    $issues = $this->runChecks($period);

    $fingerprint = $period . ':' . md5(implode('|', $issues));
    $last = (string) $this->state->get(self::STATE_KEY, '');
    if ($fingerprint === $last) {
      return $issues;
    }
    $this->state->set(self::STATE_KEY, $fingerprint);

    if (!$issues) {
      return [];
    }

    foreach ($issues as $issue) {
      $this->logger->warning('Snapshot health (@period): @issue', [
        '@period' => $period,
        '@issue' => $issue,
      ]);
    }
    $this->postToSlack($period, $issues);

    return $issues;
  }

  /**
   * Flags automated definitions with no ms_snapshot row for the period.
   */
  protected function checkDefinitionCoverage(string $period): array {
    if (!$this->database->schema()->tableExists('ms_snapshot')) {
      return ['ms_snapshot table does not exist.'];
    }

    $expected = [];
    foreach ($this->snapshotService->buildDefinitions() as $key => $definition) {
      if (($definition['acquisition'] ?? 'automated') === 'automated') {
        $expected[] = $key;
      }
    }

    $written = $this->database->select('ms_snapshot', 's')
      ->fields('s', ['definition'])
      ->condition('snapshot_date', $period)
      ->condition('snapshot_type', 'monthly')
      ->execute()
      ->fetchCol();

    if (!$written) {
      return [sprintf('No monthly snapshot recorded for %s (expected %d definitions).', $period, count($expected))];
    }

    $issues = [];
    $missing = array_diff($expected, $written);
    if ($missing) {
      $issues[] = sprintf('Missing definitions for %s: %s.', $period, implode(', ', $missing));
    }
    return $issues;
  }

  /**
   * Flags KPIs that recorded 0 (or vanished) after consistent nonzero months.
   */
  protected function checkKpiZeroDrops(string $period): array {
    $schema = $this->database->schema();
    if (!$schema->tableExists('ms_fact_kpi_snapshot')) {
      return [];
    }

    $priorPeriods = [];
    $cursor = new \DateTimeImmutable($period);
    for ($i = 1; $i <= self::ZERO_DROP_LOOKBACK; $i++) {
      $priorPeriods[] = $cursor->modify("-$i month")->format('Y-m-01');
    }

    $history = $this->loadKpiValues(array_merge([$period], $priorPeriods));
    if (empty($history[$period])) {
      // Coverage check already reports a missing snapshot.
      return [];
    }

    $issues = [];
    $kpiIds = [];
    foreach ($priorPeriods as $prior) {
      foreach (array_keys($history[$prior] ?? []) as $kpiId) {
        $kpiIds[$kpiId] = TRUE;
      }
    }

    foreach (array_keys($kpiIds) as $kpiId) {
      $priorValues = [];
      foreach ($priorPeriods as $prior) {
        if (!isset($history[$prior][$kpiId])) {
          continue 2;
        }
        $priorValues[] = (float) $history[$prior][$kpiId];
      }
      if (count(array_filter($priorValues, static fn ($v) => $v != 0.0)) < self::ZERO_DROP_LOOKBACK) {
        continue;
      }
      $current = $history[$period][$kpiId] ?? NULL;
      if ($current === NULL) {
        $issues[] = sprintf('KPI %s missing for %s after %d nonzero months (last: %s).', $kpiId, $period, self::ZERO_DROP_LOOKBACK, rtrim(rtrim(sprintf('%.4f', $priorValues[0]), '0'), '.'));
      }
      elseif ((float) $current == 0.0) {
        $issues[] = sprintf('KPI %s dropped to 0 for %s after %d nonzero months (last: %s).', $kpiId, $period, self::ZERO_DROP_LOOKBACK, rtrim(rtrim(sprintf('%.4f', $priorValues[0]), '0'), '.'));
      }
    }

    return $issues;
  }

  /**
   * Flags an org snapshot reporting zero active members.
   */
  protected function checkOrgTotals(string $period): array {
    $schema = $this->database->schema();
    if (!$schema->tableExists('ms_fact_org_snapshot')) {
      return [];
    }
    $query = $this->database->select('ms_snapshot', 's');
    $query->innerJoin('ms_fact_org_snapshot', 'o', 'o.snapshot_id = s.id');
    $query->addField('o', 'members_active');
    $query->condition('s.snapshot_date', $period);
    $query->condition('s.snapshot_type', 'monthly');
    $query->range(0, 1);
    $active = $query->execute()->fetchField();
    if ($active !== FALSE && (int) $active === 0) {
      return [sprintf('Org snapshot for %s recorded 0 active members.', $period)];
    }
    return [];
  }

  /**
   * Loads membership_totals KPI values keyed by period and KPI id.
   */
  protected function loadKpiValues(array $periods): array {
    $query = $this->database->select('ms_snapshot', 's');
    $query->innerJoin('ms_fact_kpi_snapshot', 'k', 'k.snapshot_id = s.id');
    $query->fields('s', ['snapshot_date']);
    $query->fields('k', ['kpi_id', 'metric_value']);
    $query->condition('s.snapshot_date', $periods, 'IN');
    $query->condition('s.snapshot_type', 'monthly');
    $query->condition('s.definition', 'membership_totals');

    $values = [];
    foreach ($query->execute() as $row) {
      $values[$row->snapshot_date][$row->kpi_id] = $row->metric_value;
    }
    return $values;
  }

  /**
   * Posts the issue list to Slack via the shared Slack Connector webhook.
   */
  protected function postToSlack(string $period, array $issues): void {
    $webhookUrl = $this->configFactory->get('slack_connector.settings')->get('webhook_url');
    if (empty($webhookUrl)) {
      return;
    }

    $lines = array_map(static fn (string $issue) => '• ' . $issue, $issues);
    $payload = [
      'text' => sprintf(":warning: *Snapshot health check — %s*\n%s", $period, implode("\n", $lines)),
    ];
    $channel = $this->configFactory->get('makerspace_snapshot.settings')->get('health_slack_channel');
    if (!empty($channel)) {
      $payload['channel'] = $channel;
    }

    try {
      $this->httpClient->request('POST', $webhookUrl, ['json' => $payload, 'timeout' => 10]);
    }
    catch (\Throwable $e) {
      $this->logger->error('Failed to post snapshot health alert to Slack: @message', ['@message' => $e->getMessage()]);
    }
  }

}
