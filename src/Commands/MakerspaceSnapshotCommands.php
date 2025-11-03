<?php

namespace Drupal\makerspace_snapshot\Commands;

use Drush\Commands\DrushCommands;
use Drupal\Core\Database\Connection;
use Symfony\Component\Console\Input\InputOption;
use Psr\Log\LoggerInterface;
use Drupal\makerspace_snapshot\SnapshotService;

class MakerspaceSnapshotCommands extends DrushCommands {

  protected $db;
  protected $logger;
  protected $snapshotService;

  public function __construct(Connection $db, LoggerInterface $logger, SnapshotService $snapshotService) {
    parent::__construct();
    $this->db = $db;
    $this->logger = $logger;
    $this->snapshotService = $snapshotService;
  }

  /**
   * Compute & upsert a snapshot using configured SQL.
   *
   * @command makerspace-snapshot:snapshot
   * @option snapshot-date Snapshot date (YYYY-MM-DD). Defaults to today.
   * @option snapshot-type Snapshot type (e.g., 'monthly', 'quarterly', 'annual', 'daily', 'manual').
   * @option is-test Mark this snapshot as a test snapshot.
   * @usage drush makerspace-snapshot:snapshot --snapshot-date=2025-09-30 --snapshot-type=monthly
   */
    public function snapshot(array $args = [], array $options = [
        'snapshot-date' => InputOption::VALUE_REQUIRED,
        'snapshot-type' => InputOption::VALUE_REQUIRED,
        'is-test' => InputOption::VALUE_NONE,
    ]) {
        $this->snapshotService->takeSnapshot(
            $options['snapshot-type'] ?? 'monthly',
            $options['is-test'] ?? FALSE,
            $options['snapshot-date'] ?? NULL,
            'manual_drush'
        );
  }
}
